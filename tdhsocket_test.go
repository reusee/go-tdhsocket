package tdhsocket

import (
  "testing"
  "fmt"
  "time"
  "log"
)

// mysql> create table test (id serial, i bigint(255) default null, s longblob default null, f double default null, t boolean default null, hash char(32) null default null, index(hash), index(i), index(f)) engine=innodb;

func getDb() (*Conn, error) {
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
    if err := db.Insert("test", "test", "", []string{"i"},
    []string{fmt.Sprintf("%d", time.Now().UnixNano())}); err != nil {
      t.Fail()
    }
  }
}

func TestGet(t *testing.T) {
  db, _ := getDb()
  s := fmt.Sprintf("%v", time.Now())
  n := 10
  for i := 0; i < n; i++ {
    db.Insert("test", "test", "", []string{"s"},
    []string{s})
  }
  rows, types, err := db.Get("test", "test", "id", []string{"s", "id"},
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
  count, err := db.Count("test", "test", "id", []string{"id", "s"}, 
    [][]string{[]string{""}}, GT, 0, 0, nil)
  if err != nil {
    fmt.Println(err)
  }
  fmt.Printf("Count: %d\n", count)
}

func TestUpdate(t *testing.T) {
  db, _ := getDb()
  match, change, err := db.Update("test", "test", "id", []string{"s"},
  [][]string{[]string{""}}, GT, 0, 0, nil,
  []string{"OK"})
  if err != nil {
    fmt.Println(err)
  }
  fmt.Printf("%d rows matched, %d rows changed\n", match, change)
}

func TestDelete(t *testing.T) {
  db, _ := getDb()
  change, err := db.Delete("test", "test", "id", []string{"s"},
  [][]string{[]string{""}}, GT, 0, 0, nil)
  if err != nil {
    fmt.Println(err)
  }
  fmt.Printf("%d rows deleted\n", change)
}

func TestErrorMessage(t *testing.T) {
  db, _ := getDb()
  err := db.Insert("Notest", "test", "", []string{"i"}, []string{"OK"})
  e := err.(*Error)
  if e.ClientStatus != 404 || e.ErrorCode != 1 {
    t.Fail()
  }
  fmt.Printf("Error: %s\n", err)
  err = db.Insert("test", "test", "", []string{"9"}, []string{"OK"})
  e = err.(*Error)
  if e.ClientStatus != 404 || e.ErrorCode != 3 {
    t.Fail()
  }
  fmt.Printf("Error: %s\n", err)
  err = db.Insert("test", "test", "", []string{"9"}, []string{"OK", "YES"})
  e = err.(*Error)
  if e.ClientStatus != 400 || e.ErrorCode != 7 {
    t.Fail()
  }
  fmt.Printf("Error: %s\n", err)
}

func TestBatch(t *testing.T) {
  db, _ := getDb()
  db.Batch()
  db.Insert("test", "test", "", []string{"s"}, []string{"Batch Insert"})
  db.Update("test", "test", "id", []string{"s"},
    [][]string{[]string{""}}, GT, 0, 0, nil,
    []string{"OK"})
  db.Delete("test", "test", "id", []string{"s"},
    [][]string{[]string{""}}, GT, 0, 0, nil)
  res, err := db.Commit()
  if err != nil {
    fmt.Printf("Batch error: %s\n", err)
  }
  if len(res) != 3 {
    t.Fail()
  }
  for _, r := range res {
    if r.Err != nil {
      t.Fail()
    }
  }
}
