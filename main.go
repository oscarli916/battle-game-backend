package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/websocket"
)

type TRoomStore struct {
	Mutex sync.Mutex
	Rooms map[string]*TRoom
}

func (rs *TRoomStore) Init() {
	rs.Rooms = make(map[string]*TRoom)
}

func (rs *TRoomStore) CreateRoom(roomId string) *TRoom {
	rs.Mutex.Lock()
	room := TRoom{Player1Conn: nil, Player2Conn: nil, Player1Coordinates: TCoordinates{}, Player2Coordinates: TCoordinates{}}
	rs.Rooms[roomId] = &room
	rs.Mutex.Unlock()

	return &room
}

func (rs *TRoomStore) GetRoom(roomId string) *TRoom {
	rs.Mutex.Lock()
	defer rs.Mutex.Unlock()
	room, exists := rs.Rooms[roomId]
	if !exists {
		return nil
	}
	return room
}

func (rs *TRoomStore) JoinRoom(roomId string, conn *websocket.Conn) error {
	room := rs.GetRoom(roomId)
	if room == nil {
		log.Println("room not found, creating new room with id:", roomId)
		room = RoomStore.CreateRoom(roomId)
	}
	rs.Mutex.Lock()
	if room.Player1Conn != nil && room.Player2Conn != nil {
		log.Println("room is full")
		return errors.New("room: " + roomId + " is full")
	}

	if room.Player1Conn == nil {
		room.Player1Conn = conn
		conn.WriteJSON(TMessage{MsgType: "init", Payload: map[string]interface{}{"player": "player1", "x": 64 * 5, "y": 800 - 64*2}})
	} else {
		room.Player2Conn = conn
		conn.WriteJSON(TMessage{MsgType: "init", Payload: map[string]interface{}{"player": "player2", "x": 64 * 3, "y": 800 - 64*2}})

	}
	rs.Mutex.Unlock()
	return nil
}

func (rs *TRoomStore) LeaveRoom(roomId string, conn *websocket.Conn) {
	room := rs.GetRoom(roomId)
	if room == nil {
		log.Println("room not found")
		return
	}
	rs.Mutex.Lock()
	if room.Player1Conn == conn {
		room.Player1Conn = nil
		room.Player1Coordinates = TCoordinates{}
	} else {
		room.Player2Conn = nil
		room.Player2Coordinates = TCoordinates{}
	}
	log.Println(room)
	rs.Mutex.Unlock()
}

type TRoom struct {
	Player1Conn        *websocket.Conn
	Player2Conn        *websocket.Conn
	Player1Coordinates TCoordinates
	Player2Coordinates TCoordinates
}

type TCoordinates struct {
	X int
	Y int
}

type websocketMsg struct {
	id      string
	conn    *websocket.Conn
	message TMessage
}

type TMessage struct {
	MsgType string                 `json:"msgType"`
	Payload map[string]interface{} `json:"payload"`
}

var RoomStore TRoomStore

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Accepting all requests
	},
}

var brodcast = make(chan websocketMsg)

func broadcaster() {
	for {
		msg := <-brodcast
		room := RoomStore.GetRoom(msg.id)

		RoomStore.Mutex.Lock()
		if msg.conn == room.Player1Conn {
			if room.Player2Conn != nil {
				err := room.Player2Conn.WriteJSON(msg.message)
				if err != nil {
					log.Println("error in sending message to player 2 in room"+msg.id+":", err)
				}
			}
		} else {
			if room.Player1Conn != nil {
				err := room.Player1Conn.WriteJSON(msg.message)
				if err != nil {
					log.Println("error in sending message to player 1 in room"+msg.id+":", err)
				}
			}
		}
		RoomStore.Mutex.Unlock()
	}
}

func WebSocketHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	id := query.Get("id")

	log.Println("client connected with id:", id)

	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade failed:", err)
		return
	}
	defer c.Close()

	log.Println("player joining roomId:", id)
	err = RoomStore.JoinRoom(id, c)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("player joined roomId:", id)

	go broadcaster()

	for {
		var msg websocketMsg

		err := c.ReadJSON(&msg.message)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Println("player disconnected")
				RoomStore.LeaveRoom(id, c)
			} else {
				log.Println("parsing message failed:", err)
				if websocket.IsCloseError(err, websocket.CloseGoingAway) {
					log.Println("player disconnected")
					RoomStore.LeaveRoom(id, c)
				}
			}
			break
		}

		msg.id = id
		msg.conn = c
		log.Println("received message:", msg.message)

		switch msg.message.MsgType {
		case "coordinates":
			brodcast <- msg
		}
	}
}

func main() {
	PORT := os.Getenv("PORT")
	if PORT == "" {
		log.Fatal("PORT env variable not set")
	}

	RoomStore.Init()

	http.HandleFunc("/ws", WebSocketHandler)

	log.Println("starting server on port " + PORT)
	err := http.ListenAndServe(":"+PORT, nil)
	if err != nil {
		log.Fatal(err)
	}

}
