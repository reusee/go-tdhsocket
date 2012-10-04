package tdhsocket

import (
  "testing"
  "fmt"
  "time"
  "log"
)

func getDb() (*Tdh, error) {
  db, err := New("localhost:45678", "", "")
  if err != nil {
    log.Fatal("connect error, mysql not started?")
  }
  return db, err
}

func TestNew(t *testing.T) {
  _, err := getDb()
  if err != nil {
    t.Fail()
  }
}

func TestInsert(t *testing.T) {
  db, _ := getDb()
  n := 10
  for i := 0; i < n; i++ {
    if err := db.Insert("test", "kvs", "", []string{"id", "content"},
    []string{fmt.Sprintf("%d", time.Now().UnixNano()), "OK"}); err != nil {
      t.Fail()
    }
  }
}

func TestGet(t *testing.T) {
  db, _ := getDb()
  rows, types, err := db.Get("test", "kvs", "PRIMARY", []string{"id", "content"}, 
    [][]string{[]string{""}}, GT, 0, 0, nil)
  if err != nil {
    fmt.Println(err)
  }
  for _, row := range rows {
    for _, col := range row {
      fmt.Printf("%s ", col)
    }
    print("\n")
  }
  for i, t := range types {
    fmt.Printf("Type %d: %d\n", i, t)
  }
}

func TestCount(t *testing.T) {
  db, _ := getDb()
  count, err := db.Count("test", "kvs", "PRIMARY", []string{"id", "content"}, 
    [][]string{[]string{""}}, GT, 0, 0, nil)
  if err != nil {
    fmt.Println(err)
  }
  fmt.Printf("Count: %d\n", count)
}
