package goblob

import (
	"io"
	"testing"
)

func bs() *BlobService {
	b, err := NewBlobService("localhost", "test", "fs")
	if err != nil {
		panic("no blobstore service could be created")
	}
	return b
}

func TestWrongFormatId(t *testing.T) {
	b := bs()
	_, err := b.Open("00")
	if err == nil {
		t.Fail()
	}
	t.Log("err: ", err)
}

func TestNonExistingId(t *testing.T) {
	b := bs()
	_, err := b.Open("4d88e15b60f486e428412dc9")
	if err == nil {
		t.Fail()
	}
	t.Log("err: ", err)
}

func TestOpenMongo(t *testing.T) {
	b := bs()
	t.Log("Session: ", b.s)
}

func TestRemove(t *testing.T) {
	b := bs()
	gridfile, err := b.Create("empty.txt")
	if err != nil {
		t.Fail()
	}
	t.Log("gridfile: ", gridfile)
	id := gridfile.Id()
	b.Remove(id)
	_, err = b.Open(id)
	if err == nil {
		t.Fail()
	}

}

func TestCreateGridFile(t *testing.T) {
	b := bs()
	gridfile, err := b.Create("empty.txt")
	if err != nil {
		t.Fail()
	}
	t.Log("gridfile: ", gridfile)
	b.RemoveName("empty.txt")
}

func TestWriteToGridfile(t *testing.T) {
	b := bs()
	gridfile, err := b.Create("test_file.txt")
	if err != nil {
		t.Fail()
	}
	_, err = gridfile.Write([]byte("Hello World!"))
	if err != nil {
		t.Fail()
	}
	t.Log("gridfile: ", gridfile)
	b.RemoveName("test_file.txt")
}

func TestWriteToGridfileAndClose(t *testing.T) {
	b := bs()
	gridfile, err := b.Create("second_test_file.txt")
	if err != nil {
		t.Fail()
	}
	_, err = gridfile.Write([]byte("Hello World!"))
	if err != nil {
		t.Fail()
	}
	gridfile.Close()

	t.Log("gridfile: ", gridfile)
	b.RemoveName("second_test_file.txt")
}

func TestWriteToGridfileAndCloseAndReadback(t *testing.T) {
	b := bs()
	var id1, id2 interface{}
	gridfile, err := b.Create("third.txt")
	if err != nil {
		t.Fail()
	}
	id1 = gridfile.Id()
	const hello = "Hello World!"
	_, err = gridfile.Write([]byte(hello))
	if err != nil {
		t.Fail()
	}
	gridfile.Close()
	if err != nil {
		t.Fail()
	}
	reopened, err := b.OpenName("third.txt")
	if err != nil {
		t.Fail()
	}
	id2 = reopened.Id()
	bs := make([]byte, reopened.Size())
	_, err = reopened.Read(bs)
	if err != nil {
		t.Fail()
	}
	if string(bs) != hello {
		t.Fail()
	}
	t.Log("Ids: ", id1, id2)
	if id1 != id2 {
		t.Fail()
	}
	b.RemoveName("third.txt")
}

func TestFindById(t *testing.T) {
	b := bs()
	gridfile, err := b.Create("Fourth.txt")
	if err != nil {
		t.Fail()
	}
	id1 := gridfile.Id()
	const hello = "Hello World!"
	_, err = gridfile.Write([]byte(hello))
	if err != nil {
		t.Fail()
	}
	gridfile.Close()
	if err != nil {
		t.Fail()
	}
	reopened, err := b.Open(id1)
	if err != nil {
		t.Fail()
	}
	id2 := reopened.Id()
	t.Log("id2", id2)
	t.Log("reopened: ", reopened)
	bs := make([]byte, reopened.Size())
	_, err = reopened.Read(bs)
	if err != nil {
		t.Fail()
	}
	if string(bs) != hello {
		t.Fail()
	}
	t.Log("Ids: ", id1, id2)
	if id1 != id2 {
		t.Fail()
	}
	b.RemoveName("Fourth.txt")

}

func BenchmarkWriteTinyFileSessionClose(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bs := bs()
		gridfile, err := bs.Create(string(i))
		if err != nil {
			b.Fail()
		}
		io.WriteString(gridfile, "Hello Benchmark")
		gridfile.Close()
		bs.Close()
	}
	b.StopTimer()

	bs := bs()
	for i := 0; i < b.N; i++ {
		bs.RemoveName(string(i))
	}
}

func BenchmarkWriteTinyFile(b *testing.B) {
	bs := bs()
	for i := 0; i < b.N; i++ {
		gridfile, err := bs.Create(string(i))
		if err != nil {
			b.Fail()
		}
		io.WriteString(gridfile, "Hello Benchmark")
		gridfile.Close()
	}
	b.StopTimer()

	for i := 0; i < b.N; i++ {
		bs.RemoveName(string(i))
	}
}
