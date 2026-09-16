/*
 * Copyright (C) 2018-2023 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.velocitypowered.proxy.connection.util;

import com.google.common.collect.ImmutableList;
import com.spotify.futures.CompletableFutures;
import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.proxy.server.PingOptions;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.api.proxy.server.ServerPing;
import com.velocitypowered.api.util.ModInfo;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.config.PingPassthroughMode;
import com.velocitypowered.proxy.config.VelocityConfiguration;
import com.velocitypowered.proxy.connection.client.ConnectedPlayer;
import com.velocitypowered.proxy.server.ServerPingResponse;
import com.velocitypowered.proxy.server.VelocityRegisteredServer;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import net.kyori.adventure.text.Component;

/**
 * Common utilities for handling server list ping results.
 */
public class ServerListPingHandler {

  private final VelocityServer server;

  public ServerListPingHandler(VelocityServer server) {
    this.server = server;
  }

  private ServerPing constructLocalPing(ProtocolVersion version) {
    if (version == ProtocolVersion.UNKNOWN) {
      version = ProtocolVersion.MAXIMUM_VERSION;
    }
    VelocityConfiguration configuration = server.getConfiguration();
    List<ServerPing.SamplePlayer> samplePlayers;
    if (configuration.getSamplePlayersInPing()) {
      List<ServerPing.SamplePlayer> unshuffledPlayers = server.getAllPlayers().stream()
          .map(p -> {
            if (p.getPlayerSettings().isClientListingAllowed()) {
              return new ServerPing.SamplePlayer(p.getUsername(), p.getUniqueId());
            } else {
              return ServerPing.SamplePlayer.ANONYMOUS;
            }
          })
          .collect(Collectors.toList());
      Collections.shuffle(unshuffledPlayers);
      samplePlayers = unshuffledPlayers.subList(0, Math.min(12, unshuffledPlayers.size()));
    } else {
      samplePlayers = ImmutableList.of();
    }
    return new ServerPing(
        new ServerPing.Version(version.getProtocol(),
            "Velocity " + ProtocolVersion.SUPPORTED_VERSION_STRING),
        new ServerPing.Players(server.getPlayerCount(), configuration.getShowMaxPlayers(),
            samplePlayers),
        configuration.getMotd(),
        configuration.getFavicon().orElse(null),
        configuration.isAnnounceForge() ? ModInfo.DEFAULT : null);
  }

  private CompletableFuture<ServerPingResponse> attemptPingPassthrough(VelocityInboundConnection connection,
      PingPassthroughMode mode, List<String> servers, ProtocolVersion responseProtocolVersion, String virtualHostStr) {
    ServerPingResponse fallback = new ServerPingResponse(constructLocalPing(connection.getProtocolVersion()));
    List<CompletableFuture<ServerPingResponse>> pings = new ArrayList<>();
    for (String s : servers) {
      Optional<RegisteredServer> rs = server.getServer(s);
      if (rs.isEmpty()) {
        continue;
      }
      VelocityRegisteredServer vrs = (VelocityRegisteredServer) rs.get();
      pings.add(vrs.pingInternal(connection.getConnection().eventLoop(), PingOptions.builder()
          .version(responseProtocolVersion).virtualHost(virtualHostStr).build()));
    }
    if (pings.isEmpty()) {
      return CompletableFuture.completedFuture(fallback);
    }

    CompletableFuture<List<ServerPingResponse>> pingResponses = CompletableFutures.successfulAsList(pings,
        (ex) -> fallback);
    switch (mode) {
      case ALL:
        return pingResponses.thenApply(responses -> {
          // Find the first non-fallback and keep any backend-specific extensions.
          for (ServerPingResponse response : responses) {
            if (response == fallback) {
              continue;
            }

            if (response.ping().getDescriptionComponent() == null) {
              return new ServerPingResponse(
                  response.ping().asBuilder()
                      .description(Component.empty())
                      .build(),
                  response.statusJson(),
                  response.trailingData());
            }

            return response;
          }
          return fallback;
        });
      case MODS:
        return pingResponses.thenApply(responses -> {
          // Find the first non-fallback that contains a mod list.
          for (ServerPingResponse response : responses) {
            if (response == fallback) {
              continue;
            }
            Optional<ModInfo> modInfo = response.ping().getModinfo();
            if (modInfo.isPresent()) {
              return new ServerPingResponse(
                  fallback.ping().asBuilder().mods(modInfo.get()).build(),
                  response.statusJson(),
                  response.trailingData());
            }
          }
          return fallback;
        });
      case DESCRIPTION:
        return pingResponses.thenApply(responses -> {
          // Find the first non-fallback. If it includes a modlist, add it too.
          for (ServerPingResponse response : responses) {
            if (response == fallback) {
              continue;
            }

            if (response.ping().getDescriptionComponent() == null) {
              continue;
            }

            return new ServerPingResponse(new ServerPing(
                fallback.ping().getVersion(),
                fallback.ping().getPlayers().orElse(null),
                response.ping().getDescriptionComponent(),
                fallback.ping().getFavicon().orElse(null),
                response.ping().getModinfo().orElse(null)),
                response.statusJson(),
                response.trailingData());
          }

          return fallback;
        });
      // Not possible, but covered for completeness.
      default:
        return CompletableFuture.completedFuture(fallback);
    }
  }

  /**
   * Fetches the "default" server ping for a player.
   * Returns a ServerPingResponse which includes any trailing data from the
   * backend server.
   *
   * @param connection the connection
   * @return a future with the initial ping result including trailing data
   */
  public CompletableFuture<ServerPingResponse> getInitialPing(VelocityInboundConnection connection) {
    VelocityConfiguration configuration = server.getConfiguration();
    ProtocolVersion shownVersion = connection.getProtocolVersion().isSupported()
        ? connection.getProtocolVersion()
        : ProtocolVersion.MAXIMUM_VERSION;
    PingPassthroughMode passthroughMode = configuration.getPingPassthrough();

    if (passthroughMode == PingPassthroughMode.DISABLED) {
      return CompletableFuture.completedFuture(new ServerPingResponse(constructLocalPing(shownVersion)));
    } else {
      String virtualHostStr = connection.getVirtualHost().map(InetSocketAddress::getHostString)
          .map(str -> str.toLowerCase(Locale.ROOT))
          .orElse("");
      List<String> serversToTry = server.getConfiguration().getForcedHosts().getOrDefault(
          virtualHostStr, server.getConfiguration().getAttemptConnectionOrder());
      return attemptPingPassthrough(connection, passthroughMode, serversToTry, shownVersion, virtualHostStr);
    }
  }

  /**
   * Fetches the "default" server ping for a connected player.
   * This is used for ServerDataPacket sent to already-connected players,
   * where trailing data preservation is not needed.
   *
   * @param player the connected player
   * @return a future with the server ping
   */
  public CompletableFuture<ServerPing> getInitialPing(ConnectedPlayer player) {
    ProtocolVersion shownVersion = player.getProtocolVersion().isSupported()
        ? player.getProtocolVersion()
        : ProtocolVersion.MAXIMUM_VERSION;
    return CompletableFuture.completedFuture(constructLocalPing(shownVersion));
  }
}
