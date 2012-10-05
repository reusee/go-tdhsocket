package tdhsocket

import (
  "testing"
  "fmt"
  "time"
  "log"
)

// mysql> create table test (id serial, i bigint(255) default null, s longblob default null, f double default null, t boolean default null, index(i), index(f)) engine=innodb;

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

func TestBatch(t *testing.T) {
  db, _ := getDb()
  n := fmt.Sprintf("%d", time.Now().UnixNano())
  _, err := db.Batch(
    &Request{req: &Req{INSERT, "test", "test", "id", []string{"i", "s", "f", "t"}},
      values: []string{n, "SS", "5.5", "1"}},
    &Request{req: &Req{INSERT, "test", "test", "id", []string{"i", "s", "f", "t"}},
      values: []string{n, "你好", "5.5", "1"}},
    &Request{req: &Req{INSERT, "test", "test", "id", []string{"i", "s", "f", "t"}},
      values: []string{n, "--", "5.5", "1"}},
    &Request{req: &Req{INSERT, "test", "test", "id", []string{"i", "s", "f", "t"}},
      values: []string{n, "BIG", "5.5", "1"}},
  )
  if err != nil {
    t.Fail()
  }
  count, err := db.Count("test", "test", "i", []string{"i"}, 
    [][]string{[]string{n}}, EQ, 0, 0, nil)
  if err != nil || count != 4 {
    fmt.Printf("%s\n", err)
    t.Fail()
  }
  res, err := db.Batch(
    &Request{req: &Req{UPDATE, "test", "test", "i", []string{"f"}},
      keys: [][]string{[]string{n}}, op: EQ, limit: 3,
      values: []string{"3.3"}},
    &Request{req: &Req{DELETE, "test", "test", "f", []string{"f"}},
      keys: [][]string{[]string{"3.3"}}, op: EQ},
  )
  if err != nil {
    t.Fail()
  }
  if res[0].count != 3 {
    t.Fail()
  }
  if res[1].count != 3 {
    t.Fail()
  }
}

func TestErrorMessage(t *testing.T) {
  db, _ := getDb()
  err := db.Insert("Notest", "test", "", []string{"i"}, []string{"OK"})
  fmt.Printf("Error: %s\n", err)
  err = db.Insert("test", "test", "", []string{"9"}, []string{"OK"})
  fmt.Printf("Error: %s\n", err)
  err = db.Insert("test", "test", "", []string{"9"}, []string{"OK", "YES"})
  fmt.Printf("Error: %s\n", err)
}
