package tdhsocket

import (
  "testing"
  "fmt"
)

func getDb() (*Tdh, error) {
  return New("localhost:45678")
}

func TestNew(t *testing.T) {
  _, err := getDb()
  if err != nil {
    t.Fail()
  }
}

func TestInsert(t *testing.T) {
  db, _ := getDb()
  if err := db.Insert("test", "kvs", "", []string{"id", "content"}, []string{"OK", "Hello"}); err != nil {
    fmt.Println(err)
  }
  n := 100
  for i := 0; i < n; i++ {
    if err := db.Insert("test", "kvs", "", []string{"id", "content"},
    []string{fmt.Sprintf("%d", i), "OK"}); err != nil {
      fmt.Println(err)
    }
  }
}
