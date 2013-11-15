package goblob

import "errors"
import "labix.org/v2/mgo"
import "labix.org/v2/mgo/bson"
import "time"

type BlobService struct {
	s  *mgo.Session
	db string
	fs string
}

func NewBlobService(host, db, fs string) (*BlobService, error) {
	s, err := mgo.Dial(host)
	return &BlobService{s, db, fs}, err
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
	gf *mgo.GridFile
}

func (f *File) Close() {
	f.gf.Close()
}

func (f *File) Write(b []byte) (int, error) {
	return f.gf.Write(b)
}

func (f *File) Read(b []byte) (int, error) {
	return f.gf.Read(b)
}

func (f *File) Id() string {
	blobKey := f.gf.Id()
	oid := blobKey.(bson.ObjectId)
	return oid.Hex()
}

func (f *File) MD5() (md5sum string) {
	return f.gf.MD5()
}

func (f *File) Size() int64 {
	return f.gf.Size()
}

func (f *File) UploadDate() time.Time {
	return f.gf.UploadDate()
}

func (f *File) Seek(offset int64, whence int) (pos int64, err error){
  return f.gf.Seek(offset, whence)
}

func (b *BlobService) gridfs() *mgo.GridFS {
	db := b.s.DB(b.db)
	return db.GridFS(b.fs)
}
