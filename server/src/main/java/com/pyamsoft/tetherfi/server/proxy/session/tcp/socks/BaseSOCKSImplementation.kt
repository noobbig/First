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

package com.pyamsoft.tetherfi.server.proxy.session.tcp.socks

import com.pyamsoft.pydroid.core.cast
import com.pyamsoft.pydroid.util.ifNotCancellation
import com.pyamsoft.tetherfi.core.Timber
import com.pyamsoft.tetherfi.server.ServerSocketTimeout
import com.pyamsoft.tetherfi.server.SocketCreator
import com.pyamsoft.tetherfi.server.clients.ByteTransferReport
import com.pyamsoft.tetherfi.server.clients.TetherClient
import com.pyamsoft.tetherfi.server.network.SocketBinder
import com.pyamsoft.tetherfi.server.proxy.ServerDispatcher
import com.pyamsoft.tetherfi.server.proxy.SocketTagger
import com.pyamsoft.tetherfi.server.proxy.SocketTracker
import com.pyamsoft.tetherfi.server.proxy.session.tcp.relayData
import com.pyamsoft.tetherfi.server.proxy.usingConnection
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import java.net.InetAddress
import kotlin.time.Duration.Companion.minutes
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout

internal abstract class BaseSOCKSImplementation<
    AT : BaseSOCKSImplementation.SOCKSAddressType,
    R : BaseSOCKSImplementation.Responder<AT>,
>
protected constructor(
    protected val appScope: CoroutineScope,
    protected val socketTagger: SocketTagger,
) : SOCKSImplementation<R> {

  private suspend fun connect(
      scope: CoroutineScope,
      socketCreator: SocketCreator,
      timeout: ServerSocketTimeout,
      serverDispatcher: ServerDispatcher,
      socketTracker: SocketTracker,
      networkBinder: SocketBinder.NetworkBinder,
      proxyInput: ByteReadChannel,
      proxyOutput: ByteWriteChannel,
      client: TetherClient,
      destinationAddress: InetAddress,
      destinationPort: UShort,
      addressType: AT,
      responder: R,
      onError: suspend (Throwable) -> Unit,
      onReport: suspend (ByteTransferReport) -> Unit,
  ) =
      socketCreator.create(
          type = SocketCreator.Type.CLIENT,
          onError = {
            appScope.launch(context = Dispatchers.IO) { onError(it) }
          },
          onBuild = { builder ->
            val connected =
                try {
                  // SOCKS protocol says you MUST time out after 2 minutes
                  withTimeout(2.minutes) {
                    val remote =
                        InetSocketAddress(
                            hostname = destinationAddress.hostName,
                            port = destinationPort.toInt(),
                        )

                    builder
                        .tcp()
                        .configure {
                          reuseAddress = true
                        }
                        .also { socketTagger.tagSocket() }
                        .connect(remoteAddress = remote)
                        .also { socket ->
                            // কানেক্ট হওয়ার পর নেটওয়ার্ক বাইন্ড করা
                            networkBinder.bindToNetwork(socket)
                        }
                  }
                  .also {
                    // Track this socket
                    socketTracker.track(it)
                  }
                } catch (e: Throwable) {
                  if (e is TimeoutCancellationException) {
                    Timber.w { "Timeout while waiting for socket connect()" }
                    responder.sendRefusal()
                    throw e
                  } else {
                    e.ifNotCancellation {
                      Timber.e(e) { "Error during socket connect()" }
                      responder.sendRefusal()
                      return@create
                    }
                  }
                }

            connected.use { socket ->
              // অফিসিয়াল Ktor এ সরাসরি remoteAddress প্রোপার্টি ব্যবহার করা হয়েছে
              val remote = socket.remoteAddress
              Timber.d { "SOCKS CONNECTED: $remote" }
              try {
                responder.sendConnectSuccess(
                    addressType = addressType,
                    remote = remote.cast<InetSocketAddress>(),
                )
              } catch (e: Throwable) {
                e.ifNotCancellation {
                  Timber.e(e) { "Error sending connect() SUCCESS notification" }
                  return@create
                }
              }

              socket.usingConnection(autoFlush = false) { internetInput, internetOutput ->
                try {
                  relayData(
                      scope = scope,
                      client = client,
                      proxyInput = proxyInput,
                      proxyOutput = proxyOutput,
                      internetInput = internetInput,
                      internetOutput = internetOutput,
                      serverDispatcher = serverDispatcher,
                      onReport = onReport,
                  )
                } finally {
                  // Connection cleanup
                }
              }
            }
          },
      )

  interface SOCKSAddressType
  interface Responder<AT : SOCKSAddressType> {
    suspend fun sendRefusal()
    suspend fun sendConnectSuccess(addressType: AT, remote: InetSocketAddress)
  }
}
