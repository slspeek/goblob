package goblob

import "errors"
import "io"
import "labix.org/v2/mgo"
import "labix.org/v2/mgo/bson"
import "os"

type BlobService struct {
	s  *mgo.Session
	db string
	fs string
}

func NewBlobService(s *mgo.Session, db, fs string) *BlobService {
	return &BlobService{s, db, fs}
}

func (b *BlobService) Create(fn string) (*File, error) {
	gf, err := b.gridfs().Create(fn)
	return &File{gf}, err
}

func (b *BlobService) OpenName(fn string) (*File, error) {
	gf, err := b.gridfs().Open(fn)
	return &File{gf}, err
}

func (b *BlobService) RemoveName(fn string) error {
	return b.gridfs().Remove(fn)
}

func (b *BlobService) Remove(id string) error {
	blobKey := bson.ObjectIdHex(id)
	return b.gridfs().RemoveId(blobKey)
}

func (b *BlobService) ReadFile(path string) (blobId string, err error) {
  inputf, err := os.Open(path) 
  if err != nil {
    return
  }
  outputf, err := b.Create(path) 
  if err != nil {
    return
  }
  _, err = io.Copy(outputf, inputf)
  if err != nil {
    return
  }
  blobId = outputf.StringId()
  err = outputf.Close()
  return
}

func (b *BlobService) WriteOutFile(id string, path string) (err error) {
  blobFile, err :=  b.Open(id)
  if err != nil {
    return
  }
  err = WriteFile(path, blobFile)
  return
}

func (b *BlobService) Close() {
	b.s.Close()
}

func (b *BlobService) Open(id string) (*File, error) {
	if bson.IsObjectIdHex(id) {
		blobKey := bson.ObjectIdHex(id)
		gf, err := b.gridfs().OpenId(blobKey)
		return &File{gf}, err
	} else {
		return &File{}, errors.New("Invalid hex format")
	}
}

type File struct {
	*mgo.GridFile
}

func (f *File) StringId() string {
	blobKey := f.Id()
	oid := blobKey.(bson.ObjectId)
	return oid.Hex()
}

func (b *BlobService) gridfs() *mgo.GridFS {
	db := b.s.DB(b.db)
	return db.GridFS(b.fs)
}

func WriteFile(filename string, file *File) (err error) {
	output, err := os.Create(filename)
	if err != nil {
		return
	}
	_, err = io.Copy(output, file)
	if err != nil {
		return
	}
	err = output.Close()
	return
}
