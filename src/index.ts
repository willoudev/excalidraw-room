import debug from "debug";
import express from "express";
import http from "http";
import { Server as SocketIO } from "socket.io";

import type { Socket } from "socket.io";

type UserToFollow = {
  socketId: string;
  username: string;
};
type OnUserFollowedPayload = {
  userToFollow: UserToFollow;
  action: "FOLLOW" | "UNFOLLOW";
};

const serverDebug = debug("server");
const ioDebug = debug("io");
const socketDebug = debug("socket");

require("dotenv").config(
  process.env.NODE_ENV !== "development"
    ? { path: ".env.production" }
    : { path: ".env.development" },
);

const app = express();
const port =
  process.env.PORT || (process.env.NODE_ENV !== "development" ? 80 : 3002); // default port to listen

app.use(express.static("public"));

app.get("/", (req, res) => {
  res.send("Excalidraw collaboration server is up :)");
});

const server = http.createServer(app);

server.listen(port, () => {
  serverDebug(`listening on port: ${port}`);
});

type RoomInfo = {
  name: string;
  creatorName: string;
  createdAt: number;
  // Deliberately stored (and returned by GET /rooms) so any client that
  // can see the room list can also join with one click, instead of
  // needing the original invite link. This is an explicit trade-off for
  // single-user/trusted deployments: it means this server DOES now hold
  // the E2E encryption key for rooms created this way, unlike before.
  roomKey: string;
};

// Explicit registry of rooms that were actually started through
// "create-room", as opposed to any room ID socket.io would otherwise let
// anyone join on the fly. A room ID that isn't in here (never created, or
// closed via "close-room") is rejected on "join-room" with
// "room-not-found", instead of silently letting the client sit in an
// empty room.
const activeRooms = new Map<string, RoomInfo>();

try {
  const io = new SocketIO(server, {
    transports: ["websocket", "polling"],
    cors: {
      allowedHeaders: ["Content-Type", "Authorization"],
      origin: process.env.CORS_ORIGIN || "*",
      credentials: true,
    },
    allowEIO3: true,
  });

  // Lists active collaboration rooms (room ID + name + creator + live
  // participant count + the room's E2E key). "Active" means registered
  // via create-room and not yet closed — NOT merely "someone is
  // connected right now": a room the creator left (but didn't close) is
  // still active/joinable with 0 participants, so it must still be
  // listed here. Includes the encryption key on purpose (see RoomInfo)
  // so any client that can reach this endpoint can join with one click;
  // this endpoint has no auth of its own beyond CORS_ORIGIN.
  app.get("/rooms", (req, res) => {
    res.header("Access-Control-Allow-Origin", process.env.CORS_ORIGIN || "*");
    const rooms: {
      roomId: string;
      count: number;
      name: string;
      creatorName: string;
      roomKey: string;
    }[] = [];
    activeRooms.forEach((info, roomId) => {
      const sockets = io.sockets.adapter.rooms.get(roomId);
      rooms.push({
        roomId,
        count: sockets ? sockets.size : 0,
        name: info.name,
        creatorName: info.creatorName,
        roomKey: info.roomKey,
      });
    });
    res.json({ rooms });
  });

  const joinRoomSocket = async (socket: Socket, roomID: string) => {
    socketDebug(`${socket.id} has joined ${roomID}`);
    await socket.join(roomID);
    const sockets = await io.in(roomID).fetchSockets();

    const info = activeRooms.get(roomID);
    if (info) {
      io.to(socket.id).emit("room-info", {
        name: info.name,
        creatorName: info.creatorName,
      });
    }

    if (sockets.length <= 1) {
      io.to(`${socket.id}`).emit("first-in-room");
    } else {
      socketDebug(`${socket.id} new-user emitted to room ${roomID}`);
      socket.broadcast.to(roomID).emit("new-user", socket.id);
    }

    io.in(roomID).emit(
      "room-user-change",
      sockets.map((socket) => socket.id),
    );
  };

  io.on("connection", (socket) => {
    ioDebug("connection established!");
    io.to(`${socket.id}`).emit("init-room");

    socket.on(
      "create-room",
      async (payload: {
        roomID: string;
        roomName?: string;
        creatorName?: string;
        roomKey?: string;
      }) => {
        const { roomID } = payload;
        activeRooms.set(roomID, {
          name: payload.roomName?.trim() || "Session sans nom",
          creatorName: payload.creatorName?.trim() || "Anonyme",
          createdAt: Date.now(),
          roomKey: payload.roomKey ?? "",
        });
        await joinRoomSocket(socket, roomID);
      },
    );

    socket.on("join-room", async (roomID: string) => {
      if (!activeRooms.has(roomID)) {
        socketDebug(`${socket.id} tried to join unknown/closed room ${roomID}`);
        io.to(socket.id).emit("room-not-found");
        return;
      }
      await joinRoomSocket(socket, roomID);
    });

    socket.on(
      "server-broadcast",
      (roomID: string, encryptedData: ArrayBuffer, iv: Uint8Array) => {
        socketDebug(`${socket.id} sends update to ${roomID}`);
        socket.broadcast.to(roomID).emit("client-broadcast", encryptedData, iv);
      },
    );

    socket.on("close-room", (roomID: string) => {
      socketDebug(`${socket.id} closed room ${roomID}`);
      activeRooms.delete(roomID);
      // notify everyone currently in the room, including the sender —
      // each client reacts the same way (detach locally) on receipt
      io.in(roomID).emit("room-closed");
    });

    socket.on(
      "server-volatile-broadcast",
      (roomID: string, encryptedData: ArrayBuffer, iv: Uint8Array) => {
        socketDebug(`${socket.id} sends volatile update to ${roomID}`);
        socket.volatile.broadcast
          .to(roomID)
          .emit("client-broadcast", encryptedData, iv);
      },
    );

    socket.on("user-follow", async (payload: OnUserFollowedPayload) => {
      const roomID = `follow@${payload.userToFollow.socketId}`;

      switch (payload.action) {
        case "FOLLOW": {
          await socket.join(roomID);

          const sockets = await io.in(roomID).fetchSockets();
          const followedBy = sockets.map((socket) => socket.id);

          io.to(payload.userToFollow.socketId).emit(
            "user-follow-room-change",
            followedBy,
          );

          break;
        }
        case "UNFOLLOW": {
          await socket.leave(roomID);

          const sockets = await io.in(roomID).fetchSockets();
          const followedBy = sockets.map((socket) => socket.id);

          io.to(payload.userToFollow.socketId).emit(
            "user-follow-room-change",
            followedBy,
          );

          break;
        }
      }
    });

    socket.on("disconnecting", async () => {
      socketDebug(`${socket.id} has disconnected`);
      for (const roomID of Array.from(socket.rooms)) {
        const otherClients = (await io.in(roomID).fetchSockets()).filter(
          (_socket) => _socket.id !== socket.id,
        );

        const isFollowRoom = roomID.startsWith("follow@");

        if (!isFollowRoom && otherClients.length > 0) {
          socket.broadcast.to(roomID).emit(
            "room-user-change",
            otherClients.map((socket) => socket.id),
          );
        }

        if (isFollowRoom && otherClients.length === 0) {
          const socketId = roomID.replace("follow@", "");
          io.to(socketId).emit("broadcast-unfollow");
        }
      }
    });

    socket.on("disconnect", () => {
      socket.removeAllListeners();
      socket.disconnect();
    });
  });
} catch (error) {
  console.error(error);
}
