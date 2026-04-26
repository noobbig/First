/*
 * Copyright 2025 pyamsoft
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     http://apache.org
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pyamsoft.tetherfi.server.proxy.session.tcp.http

import com.pyamsoft.pydroid.core.ThreadEnforcer
import com.pyamsoft.pydroid.util.ifNotCancellation
import com.pyamsoft.tetherfi.server.clients.ByteTransferReport
import com.pyamsoft.tetherfi.server.clients.TetherClient
import com.pyamsoft.tetherfi.server.proxy.ServerDispatcher
import com.pyamsoft.tetherfi.server.proxy.SharedProxy
import com.pyamsoft.tetherfi.server.proxy.session.tcp.AbstractTcpSessionTransport
import com.pyamsoft.tetherfi.server.proxy.session.tcp.TransportWriteCommand
import com.pyamsoft.tetherfi.server.proxy.session.tcp.relayData
import io.ktor.network.sockets.SocketTimeoutException
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.readUTF8Line // readLineStrict এর বদলে এটি ইমপোর্ট করুন
import io.ktor.utils.io.writeFully
import javax.inject.Inject
import kotlinx.coroutines.CoroutineScope

internal class HttpTransport
@Inject
internal constructor(
    private val requestParser: RequestParser,
    private val enforcer: ThreadEnforcer,
) : AbstractTcpSessionTransport<HttpProxyRequest>() {

  override val proxyType = SharedProxy.Type.HTTP

  /**
   * HTTPS Connections are encrypted and so we cannot see anything further past the initial CONNECT
   * call.
   */
  private suspend fun establishHttpsConnection(
      input: ByteReadChannel,
      output: ByteWriteChannel,
      request: HttpProxyRequest,
  ) {
    var throwaway: String?
    do {
      // পিয়ামসফটের কাস্টম readLineStrict বদলে অফিসিয়াল readUTF8Line ব্যবহার করা হয়েছে
      throwaway = input.readUTF8Line()
    } while (!throwaway.isNullOrBlank())

    debugLog { "Establish HTTPS CONNECT tunnel ${request.raw}" }
    writeHttpConnectSuccess(output)
  }

  private suspend fun replayHttpCommunication(
      output: ByteWriteChannel,
      request: HttpProxyRequest,
  ) {
    debugLog { "Rewrote initial HTTP request: ${request.raw} -> ${request.httpRequest}" }
    output.writeFully(writeHttpMessageAndAwaitMore(request.httpRequest))
  }

  override suspend fun writeProxyOutput(
      output: ByteWriteChannel,
      request: HttpProxyRequest,
      command: TransportWriteCommand,
  ) =
      when (command) {
        TransportWriteCommand.INVALID -> writeProxyHttpError(output)
        TransportWriteCommand.ERROR -> writeProxyHttpError(output)
        TransportWriteCommand.BLOCK -> writeHttpClientBlocked(output)
      }

  override suspend fun parseRequest(
      input: ByteReadChannel,
      output: ByteWriteChannel,
  ): HttpProxyRequest {
    // পিয়ামসফটের কাস্টম readLineStrict বদলে অফিসিয়াল readUTF8Line ব্যবহার করা হয়েছে
    val line = input.readUTF8Line()

    if (line.isNullOrBlank()) {
      warnLog { "No input read from proxy" }
      return INVALID_REQUEST
    }

    return requestParser.parse(line)
  }

  suspend fun exchangeInternet(
      scope: CoroutineScope,
      serverDispatcher: ServerDispatcher,
      proxyInput: ByteReadChannel,
      proxyOutput: ByteWriteChannel,
      internetInput: ByteReadChannel,
      internetOutput: ByteWriteChannel,
      request: HttpProxyRequest,
      client: TetherClient,
      onReport: suspend (ByteTransferReport) -> Unit,
  ) {
    enforcer.assertOffMainThread()

    try {
      if (request.isHttpsConnectRequest()) {
        establishHttpsConnection(
            input = proxyInput,
            output = proxyOutput,
            request = request,
        )
      } else {
        replayHttpCommunication(
            output = internetOutput,
            request = request,
        )
      }

      relayData(
          scope = scope,
          client = client,
          serverDispatcher = serverDispatcher,
          internetInput = internetInput,
          internetOutput = internetOutput,
          proxyInput = proxyInput,
          proxyOutput = proxyOutput,
          onReport = onReport,
      )
    } catch (e: Throwable) {
      e.ifNotCancellation {
        if (e is SocketTimeoutException) {
          warnLog { "Proxy:Internet socket timeout! $request $client" }
        } else {
          errorLog(e) { "Error occurred during internet exchange: $request $client" }
          writeProxyOutput(proxyOutput, request, TransportWriteCommand.ERROR)
        }
      }
    }
  }

  companion object {
    private val INVALID_REQUEST =
        HttpProxyRequest(
            file = "",
            port = 0,
            method = "",
            version = "",
            host = "",
            raw = "",
            valid = false,
        )
  }
}
