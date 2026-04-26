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
import com.pyamsoft.tetherfi.core.notification.NotificationErrorLauncher
import com.pyamsoft.tetherfi.server.ServerSocketTimeout
import com.pyamsoft.tetherfi.server.SocketCreator
import com.pyamsoft.tetherfi.server.broadcast.BroadcastNetworkStatus
import com.pyamsoft.tetherfi.server.clients.AllowedClients
import com.pyamsoft.tetherfi.server.clients.BlockedClients
import com.pyamsoft.tetherfi.server.clients.ByteTransferReport
import com.pyamsoft.tetherfi.server.clients.ClientResolver
import com.pyamsoft.tetherfi.server.clients.TetherClient
import com.pyamsoft.tetherfi.server.network.SocketBinder
import com.pyamsoft.tetherfi.server.proxy.ProxyConnectionInfo
import com.pyamsoft.tetherfi.server.proxy.ServerDispatcher
import com.pyamsoft.tetherfi.server.proxy.SharedProxy
import com.pyamsoft.tetherfi.server.proxy.SocketTagger
import com.pyamsoft.tetherfi.server.proxy.SocketTracker
import com.pyamsoft.tetherfi.server.proxy.session.tcp.TcpProxySession
import com.pyamsoft.tetherfi.server.proxy.session.tcp.TransportWriteCommand
import com.pyamsoft.tetherfi.server.proxy.usingConnection
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.SocketTimeoutException
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

@Singleton
internal class HttpProxySession
@Inject
internal constructor(
    @param:Named("app_scope") private val appScope: CoroutineScope,
    private val notificationErrorLauncher: NotificationErrorLauncher,
    private val transport: HttpTransport,
    socketTagger: SocketTagger,
    blockedClients: BlockedClients,
    clientResolver: ClientResolver,
    allowedClients: AllowedClients,
    enforcer: ThreadEnforcer,
) :
    TcpProxySession<HttpProxyRequest>(
        transport = transport,
        socketTagger = socketTagger,
        blockedClients = blockedClients,
        clientResolver = clientResolver,
        allowedClients = allowedClients,
        enforcer = enforcer,
    ) {

  override val proxyType = SharedProxy.Type.HTTP

  private suspend inline fun <T> connectToInternet(
      networkBinder: SocketBinder.NetworkBinder,
      socketCreator: SocketCreator,
      timeout: ServerSocketTimeout,
      autoFlush: Boolean,
      request: HttpProxyRequest,
      socketTracker: SocketTracker,
      noinline onError: (Throwable) -> Unit,
      crossinline block: suspend (ByteReadChannel, ByteWriteChannel) -> T,
  ): T =
      socketCreator.create(
          type = SocketCreator.Type.CLIENT,
          onError = onError,
          onBuild = { builder ->
            val remote =
                InetSocketAddress(
                    hostname = request.host,
                    port = request.port,
                )

            // অফিসিয়াল Ktor ৩.০.১ এর জন্য কোডটি পরিবর্তন করা হয়েছে
            val socket =
                builder
                    .tcp()
                    .configure {
                      reuseAddress = true
                    }
                    .also { socketTagger.tagSocket() }
                    .connect(remoteAddress = remote)

            // কানেক্ট হওয়ার পর নেটওয়ার্ক বাইন্ড করা
            networkBinder.bindToNetwork(socket)

            // Track this socket
            socketTracker.track(socket)

            return@create socket.usingConnection(autoFlush = autoFlush) {
                internetInput,
                internetOutput ->
              block(internetInput, internetOutput)
            }
          },
      )

  private suspend fun handleProxyToInternetError(
      throwable: Throwable,
      client: TetherClient,
      request: HttpProxyRequest,
      proxyOutput: ByteWriteChannel,
  ) {
    throwable.ifNotCancellation {
      if (throwable is SocketTimeoutException) {
        // Log warning
      } else {
        transport.writeProxyOutput(proxyOutput, request, TransportWriteCommand.ERROR)
      }
    }
  }

  override suspend fun proxyToInternet(
      scope: CoroutineScope,
      socketCreator: SocketCreator,
      timeout: ServerSocketTimeout,
      connectionInfo: BroadcastNetworkStatus.ConnectionInfo.Connected,
      networkBinder: SocketBinder.NetworkBinder,
      serverDispatcher: ServerDispatcher,
      proxyInput: ByteReadChannel,
      proxyOutput: ByteWriteChannel,
      proxyConnectionInfo: ProxyConnectionInfo,
      socketTracker: SocketTracker,
      client: TetherClient,
      request: HttpProxyRequest,
      onReport: suspend (ByteTransferReport) -> Unit,
  ) {
    enforcer.assertOffMainThread()

    try {
      connectToInternet(
          autoFlush = true,
          socketCreator = socketCreator,
          timeout = timeout,
          networkBinder = networkBinder,
          socketTracker = socketTracker,
          request = request,
          onError = { e ->
            appScope.launch(context = Dispatchers.IO) {
              handleProxyToInternetError(
                  throwable = e,
                  proxyOutput = proxyOutput,
                  request = request,
                  client = client,
              )
              notificationErrorLauncher.showError(e)
            }
          },
          block = { internetInput, internetOutput ->
            // আপনার বাকি লজিক এখানে থাকবে
          },
      )
    } catch (e: Throwable) {
        e.ifNotCancellation {
            notificationErrorLauncher.showError(e)
        }
    }
  }
}
