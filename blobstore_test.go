package goblob

import (
	"fmt"
	"io"
	"io/ioutil"
	"labix.org/v2/mgo"
	"os"
	"testing"
)

func bs() *BlobService {
	s, err := mgo.Dial("localhost")
	if err != nil {
		panic("could not connect to mongo")
	}
	b := NewBlobService(s, "test", "fs")
	return b
}

func TestWrongFormatId(t *testing.T) {
	b := bs()
	defer b.Close()
	_, err := b.Open("00")
	if err == nil {
		t.Fail()
	}
	t.Log("err: ", err)
}

func TestNonExistingId(t *testing.T) {
	b := bs()
	defer b.Close()
	_, err := b.Open("4d88e15b60f486e428412dc9")
	if err == nil {
		t.Fail()
	}
	t.Log("err: ", err)
}

func TestNonExistingName(t *testing.T) {
	b := bs()
	defer b.Close()
	_, err := b.OpenName("FooBar")
	if err == nil {
		t.Fail()
	}
	t.Log("err: ", err)
}

func TestOpenMongo(t *testing.T) {
	b := bs()
	defer b.Close()
	t.Log("Session: ", b.s)
}

func TestRemove(t *testing.T) {
	b := bs()
	defer b.Close()
	gridfile, err := b.Create("empty.txt")
	if err != nil {
		t.Fail()
	}
	t.Log("gridfile: ", gridfile)
	id := gridfile.StringId()
	b.Remove(id)
	_, err = b.Open(id)
	if err == nil {
		t.Fail()
	}

}

func TestCreateGridFile(t *testing.T) {
	b := bs()
	defer b.Close()
	gridfile, err := b.Create("empty.txt")
	if err != nil {
		t.Fail()
	}
	t.Log("gridfile: ", gridfile)
	b.RemoveName("empty.txt")
}

func TestWriteToGridfile(t *testing.T) {
	b := bs()
	defer b.Close()
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
	defer b.Close()
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
	defer b.Close()
	var id1, id2 interface{}
	gridfile, err := b.Create("third.txt")
	if err != nil {
		t.Fail()
	}
	id1 = gridfile.StringId()
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
	id2 = reopened.StringId()
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
	defer b.Close()
	gridfile, err := b.Create("Fourth.txt")
	if err != nil {
		t.Fail()
	}
	id1 := gridfile.StringId()
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
	id2 := reopened.StringId()
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

func TestWriteFile(t *testing.T) {
	b := bs()
	defer b.Close()
	gridfile, err := b.Create("Fourth.txt")
	if err != nil {
		t.Fail()
	}
	id1 := gridfile.StringId()
	const hello = "Hello World!"
	_, err = gridfile.Write([]byte(hello))
	if err != nil {
		t.Fail()
	}
	gridfile.Close()
	if err != nil {
		t.Fail()
	}

	reopenedGf, err := b.Open(id1)
	if err != nil {
		t.Fail()
	}
	err = WriteFile("fourth_tmp", reopenedGf)
	reopenedGf.Close()
	file, err := os.Open("fourth_tmp")
	if err != nil {
		t.Fail()
	}
	t.Log("file: ", file)
	file.Close()
	os.Remove("fourth_tmp")

}

func TestWriteOutFile(t *testing.T) {
	b := bs()
	defer b.Close()
	gridfile, err := b.Create("Fourth.txt")
	if err != nil {
		t.Fail()
	}
	id := gridfile.StringId()
	const hello = "Hello World!"
	_, err = gridfile.Write([]byte(hello))
	if err != nil {
		t.Fail()
	}
	err = gridfile.Close()
	if err != nil {
		t.Fail()
	}

	err = b.WriteOutFile(id, "fourth_tmp")
	if err != nil {
		t.Fail()
	}
	file, err := os.Open("fourth_tmp")
	if err != nil {
		t.Fail()
	}
	t.Log("file: ", file)
	data, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fail()
	}
	if string(data) != hello {
		t.Fatal("Expected: ", hello, " Got: ", data)
	}
	file.Close()
	os.Remove("fourth_tmp")
}

func TestReadFile(t *testing.T) {
	b := bs()
	defer b.Close()
	const hello = "Hello World!"
	diskf, err := os.Create("input.txt")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(diskf, hello)
	err = diskf.Close()
	if err != nil {
		t.Fatal(err)
	}

	id, err := b.ReadFile("input.txt")
	if err != nil {
		t.Fatal(err)
	}

	reopenedGf, err := b.Open(id)
	if err != nil {
		t.Fail()
	}
	data, err := ioutil.ReadAll(reopenedGf)
	if err != nil {
		t.Fail()
	}
	if string(data) != hello {
		t.Fatal("Expected: ", hello, " Got: ", data)
	}
	os.Remove("input.txt")

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
