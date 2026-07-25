package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v2/internal/model"
	"github.com/gorilla/websocket"
)

func main() {
	port := flag.String("port", "8080", "server port")
	uid := flag.String("uid", "", "user id")
	name := flag.String("name", "", "username")
	room := flag.String("room", "room1", "room id")
	flag.Parse()

	if *uid == "" {
		*uid = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	if *name == "" {
		*name = *uid
	}

	u := url.URL{Scheme: "ws", Host: "127.0.0.1:" + *port, Path: "/ws"}
	q := u.Query()
	q.Set("uid", *uid)
	q.Set("name", *name)
	q.Set("room", *room)
	u.RawQuery = q.Encode()

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatalf("[client] connect failed: %v", err)
	}
	defer conn.Close()

	log.Printf("[client] connected as uid=%s name=%s room=%s", *uid, *name, *room)
	log.Println("[client] type text and press Enter to send danmaku")

	done := make(chan struct{})
	go readLoop(conn, done)
	go inputLoop(conn)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	select {
	case <-done:
	case <-interrupt:
		log.Println("[client] interrupted, closing")
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"))
	}
}

func readLoop(conn *websocket.Conn, done chan<- struct{}) {
	defer close(done)

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[client] read stopped: %v", err)
			return
		}

		var packet model.Packet
		if err := json.Unmarshal(raw, &packet); err != nil {
			log.Printf("[client] invalid packet: %v", err)
			continue
		}

		switch packet.Type {
		case model.TypeDanmaku:
			var msg model.Danmaku
			if err := json.Unmarshal(packet.Data, &msg); err != nil {
				log.Printf("[client] invalid danmaku: %v", err)
				continue
			}
			log.Printf("[room:%s] %s(%s): %s", msg.RoomID, msg.Username, msg.UserID, msg.Content)
		default:
			log.Printf("[client] unknown packet type: %d", packet.Type)
		}
	}
}

func inputLoop(conn *websocket.Conn) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		payload, err := json.Marshal(model.Danmaku{Content: text})
		if err != nil {
			log.Printf("[client] marshal input failed: %v", err)
			continue
		}

		packet := model.Packet{
			Type: model.TypeDanmaku,
			Data: payload,
		}
		if err := conn.WriteJSON(packet); err != nil {
			log.Printf("[client] send failed: %v", err)
			return
		}
	}
}
