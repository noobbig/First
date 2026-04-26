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

package com.pyamsoft.tetherfi.server.proxy.session.tcp.socks.five

import com.pyamsoft.pydroid.util.ifNotCancellation
import com.pyamsoft.tetherfi.server.ServerSocketTimeout
import com.pyamsoft.tetherfi.server.SocketCreator
import com.pyamsoft.tetherfi.server.clients.TetherClient
import com.pyamsoft.tetherfi.server.proxy.ServerDispatcher
import com.pyamsoft.tetherfi.server.proxy.SocketTagger
import com.pyamsoft.tetherfi.server.proxy.SocketTracker
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.BoundDatagramSocket
import io.ktor.network.sockets.Datagram
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.aSocket
import io.ktor.utils.io.core.ByteReadPacket
import javax.inject.Inject
import javax.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch

@Singleton
internal class UDPRelayServer
@Inject
internal constructor(
    private val serverDispatcher: ServerDispatcher,
    private val socketTagger: SocketTagger,
    private val socketTracker: SocketTracker,
) {

  private suspend fun bindUdp(
      socketCreator: SocketCreator,
      host: String,
      port: Int,
  ): BoundDatagramSocket {
    val remote = InetSocketAddress(host, port)
    
    // অফিসিয়াল Ktor ৩.০.১ এর জন্য bindWithConfiguration বদলে bind ব্যবহার করা হয়েছে
    return aSocket(SelectorManager(serverDispatcher.primary))
        .udp()
        .bind(localAddress = remote)
        .also { socketTagger.tagSocket() }
  }

  suspend fun relay(
      scope: CoroutineScope,
      socketCreator: SocketCreator,
      timeout: ServerSocketTimeout,
      host: String,
      port: Int,
      client: TetherClient,
  ) {
    try {
      val socket = bindUdp(
          socketCreator = socketCreator,
          host = host,
          port = port,
      )

      socketTracker.track(socket)

      scope.launch(context = serverDispatcher.primary) {
        try {
          // UDP রিলে লজিক এখানে থাকবে
          // অফিসিয়াল Ktor এ send এবং receive সরাসরি সকেটে থাকে
          while (true) {
              val datagram = socket.receive()
              // রিলে ডেটা প্রসেস করুন
          }
        } catch (e: Throwable) {
          e.ifNotCancellation {
            // Handle error
          }
        }
      }
    } catch (e: Throwable) {
      e.ifNotCancellation {
        // Handle binding error
      }
    }
  }
}
