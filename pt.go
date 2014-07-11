package pt

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"strings"
	"io"

	"code.google.com/p/leveldb-go/leveldb"
	"code.google.com/p/leveldb-go/leveldb/db"
)

func _msg() string {
	var msg string
	if _, fname, lineno, ok := runtime.Caller(2); !ok {
		debug("couldn't get line number")
	} else {
		j := strings.LastIndex(fname, "/")
		fname = fname[j+1:]
		msg = fmt.Sprintf("./%s:%d ", fname, lineno)
	}
	return msg
}

func Error(i ...interface{}) {
	msg := _msg()
	log.Printf(msg)
	log.Println(i...)
}

func debug(i ...interface{}) {
	msg := _msg()
	fmt.Printf(msg)
	fmt.Println(i...)
}

func debugf(format string, i ...interface{}) {
	msg := _msg()
	fmt.Printf(msg+format, i...)
}

type Node struct {
	Id       int            `json:"id"`
	Name     string         `json:"name"`
	Value    []string       `json:"value"`
	Children map[string]int `json:"children"`
	Parent   int            `json:"parent"`
	Edgename string         `json:"edgename"`
}

type Tree struct {
	*leveldb.DB
	Root *Node
	Newid chan int
	closed bool
}

func int2b(a int) []byte {
	b := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(b, uint32(a))
	return b
}

func (t *Tree) Get(ro *db.ReadOptions, id int) (*Node, error) {
	b := int2b(id)
	dat, err := t.DB.Get(b, ro)
	if err != nil {
		return nil, err
	}
	res := new(Node)
	json.Unmarshal(dat, res)
	return res, nil

}

func (t *Tree) Put(wo *db.WriteOptions, n *Node) error {
	if dat, err := json.Marshal(n); err != nil {
		return err
	} else {
		b := int2b(n.Id)
		if err = t.DB.Set(b, dat, wo); err != nil {
			return err
		}
	}
	return nil
}

func NewTree(dbname string) *Tree {
	debug("New tree")
	n := new(Tree)
	n.closed = false
	d, err := leveldb.Open(dbname, nil)
	if err != nil {
		Error(err)
	}
	n.DB = d
	if n.Root, err = n.Get(nil, 0); err != nil {
		debug("could not get", err)
		n.Root = &Node{Parent: 0}
		if err := n.Put(nil, n.Root); err != nil {
			Error("could not set", err)
		}
	}
	n.Newid = make(chan int)
	go func() {
		id := n.Root.Parent + 1
		for ; ; id++ {
			n.Newid <- id
			if n.closed {
				break
			}
		}
		log.Println("exiting Newid goroutine")
	}()
	return n
}

func (t *Tree) Close() {
	// Root's parent contains the maxid
	t.Root.Parent = <-t.Newid
	t.Root.Parent--
	if err := t.Put(nil, t.Root); err != nil {
		Error(err)
	}
	t.closed = true
	t.DB.Close()
}

// returns the matching prefix between the two
func matchprefix(a, b string) string {
	res := ""
	smallest := len(a)
	if smallest > len(b) {
		smallest = len(b)
	}
	for i := 0; i < smallest; i++ {
		if a[i] == b[i] {
			res += string(a[i])
		} else {
			break
		}
	}
	return res
}

func (t *Tree) fetchChild(ro *db.ReadOptions, n *Node, s string) *Node {
	newid, ok := n.Children[s]
	if !ok {
		return nil
	}
	newnode, err := t.Get(ro, newid)
	if err != nil {
		Error(err)
		return nil
	}
	return newnode
}

/*
	Lookup takes the node to start from, the string to search for, and a
	count of how many chars are matched already.

	It returns the node that matches most closely and the number of
	characters (starting from 0) that match.

*/
func (t *Tree) Lookup(n *Node, search string, i int) (*Node, int) {
	res, i := t.LookupOpt(nil, n, search, i)
	return res, i
}

func (t *Tree) LookupOpt(opt *db.ReadOptions, n *Node, search string, i int) (*Node, int) {
	debugf("Lookup(%+v, \"%s\", %d)\n", n, search, i)
	if n == nil {
		return nil, i
	}
	child := t.fetchChild(opt, n, string(search[i]))
	if child == nil {
		return n, i
	}	
	match := matchprefix(child.Edgename, search[i:])
	i += len(match)
	if i < len(search) && child.Edgename == match {
		c, i := t.LookupOpt(opt, child, search, i)
		if c != nil {
			return c, i
		}
	}
	return child, i
}

func (t *Tree) addChild(wo *db.WriteOptions, parent *Node, edgename, name string, value []string) error {
	child := new(Node)
	child.Id = <-t.Newid
	child.Parent = parent.Id
	child.Edgename = edgename
	child.Name = name
	child.Value = value
	if parent.Children == nil {
		parent.Children = make(map[string]int)
	}
	parent.Children[string(edgename[0])] = child.Id
	err := t.Put(wo, parent)
	if err != nil {
		return err
	}
	err = t.Put(wo, child)
	if err != nil {
		return err
	}
	return nil
}

func (t *Tree) Insert(k, v string) {
	t.InsertOpt(nil, nil, k, v)
}

func (t *Tree) InsertOpt(wo *db.WriteOptions, ro *db.ReadOptions, k, v string) {
	debug("insert", k, v)
	n, i := t.LookupOpt(ro, t.Root, k, 0)
	commonprefix := k[:i]
	debug("Insert", k, "and commonprefix is", commonprefix)

	debugf("Lookup returns node '%+v' mathced chars = '%v' match '%v'\n", n, i, k[:i])

	debug("is it the root?")
	if n == t.Root {
		debug("addChild")
		t.addChild(wo, t.Root, k, k, []string{v})
		debug("yes")
		return
	}
	debug("no")

	debug("is it an exact match?")
	if k == n.Name {
		n.Value = append(n.Value, v)
		debug("node", n, "already found append value here TODO")
		debug("yes")
		t.Put(wo, n)
		return
	}
	debug("no")

	debug("does commonprefix match the whole edgename and more?")
	if commonprefix == n.Name {
		// but if it is longer such as "alibaba" and we add "alibabas" it's a simple add
		if len(k) > len(n.Name) {
			e := k[len(commonprefix):]
			log.Println(e)
			t.addChild(wo, n, e, k, []string{v})
			debug("yes")
			return
		}
	}
	debug("no")

	// otherwise it's a split because it matches part of n.Edgename

	debug("split them then")

	/* say we have the string "key" and we add "ketones"
	   then the left node will be "y" the right node will be "tones"
	   and the middle will be "ke"
	*/

	mid := n
	children := make(map[string]int)

	// whatever is left in n.Name after taking out the length of common prefix
	lname := n.Name[len(commonprefix):]
	rname := k[len(commonprefix):]

	midparent, err := t.Get(ro, n.Parent)
	if err != nil {
		Error("Couldn't find parent of", n, err)
	}

	midedgename := n.Name[len(midparent.Name):len(commonprefix)]

	debugf("mid \"%s\" left \"%s\" right \"%s\"\n", commonprefix, lname, rname)

	// left node (preserve the old string)
	leftnode := new(Node)
	leftnode.Value = n.Value
	leftnode.Edgename = lname
	leftnode.Name = mid.Name
	leftnode.Id = <-t.Newid
	leftnode.Children = mid.Children
	children[string(leftnode.Edgename[0])] = leftnode.Id

	// update the middle node
	mid.Edgename = midedgename
	mid.Name = commonprefix
	leftnode.Parent = mid.Id

	if rname == "" {
		// if no right node them mid will have the value
		mid.Value = append(mid.Value, v)
	} else {
		// if you have 'cats' and try to add 'cat'
		// you'll have this empty right node case
		mid.Value = []string{}
		rightnode := new(Node)
		rightnode.Value = append(rightnode.Value, v)
		rightnode.Edgename = rname
		rightnode.Name = k
		rightnode.Id = <-t.Newid
		children[string(rightnode.Edgename[0])] = rightnode.Id
		rightnode.Parent = mid.Id
		t.Put(wo, rightnode)
	}

	mid.Children = children

	// also update mid's parent id
	delete(midparent.Children, string(n.Edgename[0]))
	midparent.Children[string(midedgename[0])] = mid.Id

	t.Put(wo, midparent)
	t.Put(wo, mid)
	t.Put(wo, leftnode)
}

func (t *Tree) Print(w io.Writer, n *Node, prefix string) {
	cb := func(prefix string, val []string) {
		fmt.Fprintf(w, "%s %s\n", prefix, val)
	}
	t.Dfs(nil,  n, prefix, cb)
}

func (t *Tree) Dfs(ro *db.ReadOptions, n *Node, prefix string, cb func(prefix string, value []string)) {
	dn := n
	if len(dn.Children) == 0 {
		cb(prefix, n.Value)
	} else {
		for _, c := range dn.Children {
			cnode, err := t.Get(ro, c)
			if err != nil {
				Error(err)
			}
			t.Dfs(ro,cnode, prefix+cnode.Edgename, cb)
		}
		if len(n.Value) != 0 {
			cb(prefix, n.Value)
		}
	}
}
