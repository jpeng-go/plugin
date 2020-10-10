// Copyright Fuzamei Corp. 2018 All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/33cn/chain33/common/log/log15"
	"google.golang.org/grpc"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/33cn/chain33/common"
	"github.com/33cn/chain33/common/address"
	"github.com/33cn/chain33/common/crypto"
	rpctypes "github.com/33cn/chain33/rpc/types"
	"github.com/33cn/chain33/types"
	ty "github.com/33cn/plugin/plugin/dapp/valnode/types"
)

const fee = 1e6
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

var r *rand.Rand

// TxHeightOffset needed
var TxHeightOffset int64

func main() {
	if len(os.Args) == 1 || os.Args[1] == "-h" {
		LoadHelp()
		return
	}
	fmt.Println("jrpc url:", os.Args[2]+":8801")
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
	argsWithoutProg := os.Args[1:]
	switch argsWithoutProg[0] {
	case "-h": //使用帮助
		LoadHelp()
	case "perf":
		if len(argsWithoutProg) != 6 {
			fmt.Print(errors.New("参数错误").Error())
			return
		}
		Perf(argsWithoutProg[1], argsWithoutProg[2], argsWithoutProg[3], argsWithoutProg[4], argsWithoutProg[5])
	case "put":
		if len(argsWithoutProg) != 3 {
			fmt.Print(errors.New("参数错误").Error())
			return
		}
		Put(argsWithoutProg[1], argsWithoutProg[2], nil)
	case "get":
		if len(argsWithoutProg) != 3 {
			fmt.Print(errors.New("参数错误").Error())
			return
		}
		Get(argsWithoutProg[1], argsWithoutProg[2])
	case "valnode":
		if len(argsWithoutProg) != 4 {
			fmt.Print(errors.New("参数错误").Error())
			return
		}
		ValNode(argsWithoutProg[1], argsWithoutProg[2], argsWithoutProg[3])
	}
}

// LoadHelp ...
func LoadHelp() {
	fmt.Println("Available Commands:")
	fmt.Println("perf [ip, size, num, interval, duration]                     : 写数据性能测试")
	fmt.Println("put  [ip, size]                                              : 写数据")
	fmt.Println("get  [ip, hash]                                              : 读数据")
	fmt.Println("valnode [ip, pubkey, power]                                  : 增加/删除/修改tendermint节点")
}

// Perf ...
func Perf(ip, txsize, num, sleepinterval, totalduration string) {
	var numThread int
	numInt, err := strconv.Atoi(num)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	intervalInt, err := strconv.Atoi(sleepinterval)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	durInt, err := strconv.Atoi(totalduration)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	sizeInt, _ := strconv.Atoi(txsize)
	if numInt < 10 {
		numThread = 1
	} else if numInt > 100 {
		numThread = 10
	} else {
		numThread = numInt / 10
	}
	maxTxPerAcc := 100000
	ch := make(chan struct{}, numThread)
	payload := RandStringBytes(sizeInt)
	for i := 0; i < numThread; i++ {
		go func() {

			conn, err := grpc.Dial(ip+":8802", grpc.WithInsecure())
			if err != nil {
				log.Error("grpc dial", "err", err)
				return
			}
			defer conn.Close()
			gcli := types.NewChain33Client(conn)

			txCount := 0
			_, priv := genaddress()
			for sec := 0; durInt == 0 || sec < durInt; {
				height := getHeight(gcli)
				payload = RandStringBytes(sizeInt)
				tx := &types.Transaction{Execer: []byte("user.write"), Payload: types.Str2Bytes(payload)}
				tx.To = execAddr
				tx.Nonce = rand.Int63()
				tx.Expire = height + types.TxHeightFlag + types.LowAllowPackHeight
				for txs := 0; txs < numInt/numThread; txs++ {
					if txCount >= maxTxPerAcc {
						_, priv = genaddress()
						txCount = 0
					}
					tx.Nonce += int64(txs)
					tx.Sign(types.SECP256K1, priv)
					_, err := gcli.SendTransaction(context.Background(), tx)
					if err != nil {
						log.Error("sendtx", "err", err)
					}
					txCount++
				}
				time.Sleep(time.Second * time.Duration(intervalInt))
				sec += intervalInt
			}
			ch <- struct{}{}
		}()
	}
	for j := 0; j < numThread; j++ {
		<-ch
	}
}

var (
	log = log15.New()
	execAddr = address.ExecAddress("user.write")
)

func getHeight(gcli types.Chain33Client) int64 {
	header, err := gcli.GetLastHeader(context.Background(), &types.ReqNil{})
	if err != nil {
		log.Error("getHeight", "err", err)
	}
	return header.Height
}

func send(gcli types.Chain33Client, height int64, tx *types.Transaction, privkey crypto.PrivKey) {

	tx.Sign(types.SECP256K1, privkey)
	reply, err := gcli.SendTransaction(context.Background(), tx)
	if err != nil {
		log.Error("sendtx", "err", err)
	}else {
		log.Debug("sendtx", "hash", common.ToHex(reply.Msg))
	}
}

// Put ...
func Put(ip string, size string, privkey crypto.PrivKey) {
	sizeInt, err := strconv.Atoi(size)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	url := "http://" + ip + ":8801"
	if privkey == nil {
		_, privkey = genaddress()
	}
	payload := RandStringBytes(sizeInt)
	//fmt.Println("payload:", common.ToHex([]byte(payload)))

	tx := &types.Transaction{Execer: []byte("user.write"), Payload: []byte(payload), Fee: 1e6}
	tx.To = address.ExecAddress("user.write")
	tx.Expire = TxHeightOffset + types.TxHeightFlag + types.LowAllowPackHeight
	tx.Sign(types.SECP256K1, privkey)
	poststr := fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"Chain33.SendTransaction","params":[{"data":"%v"}]}`,
		common.ToHex(types.Encode(tx)))

	resp, err := http.Post(url, "application/json", bytes.NewBufferString(poststr))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Debug("sendtx", "result", string(b))
	//fmt.Printf("returned JSON: %s\n", string(b))
}

// Get ...
func Get(ip string, hash string) {
	url := "http://" + ip + ":8801"
	fmt.Println("transaction hash:", hash)

	poststr := fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"Chain33.QueryTransaction","params":[{"hash":"%s"}]}`, hash)
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(poststr))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("returned JSON: %s\n", string(b))
}

func setTxHeight(ip string) {
	url := "http://" + ip + ":8801"
	poststr := fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"Chain33.GetLastHeader","params":[]}`)
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(poststr))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	//fmt.Printf("returned JSON: %s\n", string(b))
	msg := &RespMsg{}
	err = json.Unmarshal(b, msg)
	if err != nil {
		fmt.Println(err)
		return
	}
	TxHeightOffset = msg.Result.Height + types.LowAllowPackHeight
	fmt.Println("TxHeightOffset:", TxHeightOffset)
}

// RespMsg ...
type RespMsg struct {
	ID     int64           `json:"id"`
	Result rpctypes.Header `json:"result"`
	Err    string          `json:"error"`
}

func getprivkey(key string) crypto.PrivKey {
	cr, err := crypto.New(types.GetSignName("", types.SECP256K1))
	if err != nil {
		panic(err)
	}
	bkey, err := common.FromHex(key)
	if err != nil {
		panic(err)
	}
	priv, err := cr.PrivKeyFromBytes(bkey)
	if err != nil {
		panic(err)
	}
	return priv
}

func genaddress() (string, crypto.PrivKey) {
	cr, err := crypto.New(types.GetSignName("", types.SECP256K1))
	if err != nil {
		panic(err)
	}
	privto, err := cr.GenKey()
	if err != nil {
		panic(err)
	}
	addrto := address.PubKeyToAddress(privto.PubKey().Bytes())
	fmt.Println("addr:", addrto.String())
	return addrto.String(), privto
}

// RandStringBytes ...
func RandStringBytes(n int) string {
	b := make([]byte, n)
	rand.Seed(time.Now().UnixNano())
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// ValNode ...
func ValNode(ip, pubkey, power string) {
	url := "http://" + ip + ":8801"

	fmt.Println(pubkey, ":", power)
	pubkeybyte, err := hex.DecodeString(pubkey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	powerInt, err := strconv.Atoi(power)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	_, priv := genaddress()
	privkey := common.ToHex(priv.Bytes())
	nput := &ty.ValNodeAction_Node{Node: &ty.ValNode{PubKey: pubkeybyte, Power: int64(powerInt)}}
	action := &ty.ValNodeAction{Value: nput, Ty: ty.ValNodeActionUpdate}
	tx := &types.Transaction{Execer: []byte("valnode"), Payload: types.Encode(action), Fee: fee}
	tx.To = address.ExecAddress("valnode")
	tx.Nonce = r.Int63()
	tx.Sign(types.SECP256K1, getprivkey(privkey))

	poststr := fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"Chain33.SendTransaction","params":[{"data":"%v"}]}`,
		common.ToHex(types.Encode(tx)))

	resp, err := http.Post(url, "application/json", bytes.NewBufferString(poststr))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("returned JSON: %s\n", string(b))
}
