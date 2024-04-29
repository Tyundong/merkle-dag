package merkledag

import (
	"encoding/binary"
	"encoding/json"
	"hash"
)

const (
	K          = 1 << 10
	BLOCK_SIZE = 256 * K
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  [][]byte
}

var Stack = make([]Object, 0)

type ListNode struct {
	Hash []byte
	Next []byte
}

func (n *ListNode) Bytes() []byte {
	buf := make([]byte, len(n.Hash)+len(n.Next)+16)
	binary.BigEndian.PutUint64(buf, uint64(len(n.Hash)))
	copy(buf[8:], n.Hash)
	binary.BigEndian.PutUint64(buf[8+len(n.Hash):], uint64(len(n.Next)))
	copy(buf[16+len(n.Hash):], n.Next)
	return buf
}
func Add(store KVStore, node Node, h hash.Hash) []byte {
	switch node.Type() {
	case FILE:
		file := node.(File)
		fileHash, data := StoreFile(store, file, h)
		obj := Stack[len(Stack)-1]
		obj.Links = append(obj.Links, Link{
			Name: file.Name(),
			Hash: fileHash,
			Size: int(file.Size()),
		})
		obj.Data = append(obj.Data, data)
	case DIR:
		dir := node.(Dir)
		obj := Object{
			Links: make([]Link, 0),
			Data:  make([][]byte, 0),
		}
		Stack = append(Stack, obj)
		dirHash, data := StoreDir(store, dir, h)
		obj = Stack[len(Stack)-1]
		Stack = Stack[:len(Stack)-1]
		obj.Links = append(obj.Links, Link{
			Name: dir.Name(),
			Hash: dirHash,
			Size: int(dir.Size()),
		})
		obj.Data = append(obj.Data, data)
		h.Reset()
		objBytes, _ := json.Marshal(obj)
		h.Write(objBytes)
		objHash := h.Sum(nil)
		store.Put(objHash, objBytes)
		return objHash
	}
	return nil
}

func StoreFile(store KVStore, node File, h hash.Hash) ([]byte, []byte) {
	t := []byte("blob")
	if node.Size() > BLOCK_SIZE {
		t = []byte("list")
	}
	n := (node.Size() + BLOCK_SIZE - 1) / BLOCK_SIZE
	var headHash []byte = nil
	for i := 0; i < int(n); i++ {
		data := node.Bytes()[uint64(i*BLOCK_SIZE):uint64((i+1)*BLOCK_SIZE)]
		h.Reset()
		h.Write(data)
		fileHash := h.Sum(nil)
		store.Put(fileHash, data)
		listNode := ListNode{Hash: fileHash, Next: headHash}
		h.Reset()
		h.Write(listNode.Bytes())
		listNodeHash := h.Sum(nil)
		store.Put(listNodeHash, listNode.Bytes())
		headHash = listNodeHash
	}
	return headHash, t
}

func StoreDir(store KVStore, dir Dir, h hash.Hash) ([]byte, []byte) {
	t := []byte("tree")
	tree := Object{
		Links: make([]Link, 0),
		Data:  make([][]byte, 0),
	}
	it := dir.It()
	for it.Next() {
		node := it.Node()
		switch node.Type() {
		case FILE:
			file := node.(File)
			fileHash, _ := StoreFile(store, file, h)
			tree.Links = append(tree.Links, Link{
				Name: file.Name(),
				Hash: fileHash,
				Size: int(file.Size()),
			})
		case DIR:
			subDir := node.(Dir)
			subDirHash, _ := StoreDir(store, subDir, h)
			tree.Links = append(tree.Links, Link{
				Name: subDir.Name(),
				Hash: subDirHash,
				Size: int(subDir.Size()),
			})
		}
	}
	h.Reset()
	treeBytes, _ := json.Marshal(tree)
	h.Write(treeBytes)
	treeHash := h.Sum(nil)
	err := store.Put(treeHash, treeBytes)
	if err != nil {
		return nil, nil
	}
	return treeHash, t
}
