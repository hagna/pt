package pt

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/jmhodges/levigo"
	"log"
	"runtime"
	"strings"
)

func _msg() string {
	var msg string
	if _, fname, lineno, ok := runtime.Caller(2); !ok {
		debug("couldn't get line number")
	} else {
		j := strings.LastIndex(fname, "/")
		fname = fname[j+1:]
		msg = fmt.Sprintf("%s:%d ", fname, lineno)
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
	*levigo.DB
	Root *Node
	Newid chan int
}

func int2b(a int) []byte {
	b := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(b, uint32(a))
	return b
}

func (t *Tree) Get(ro *levigo.ReadOptions, id int) (*Node, error) {
	debug("Get")
	b := int2b(id)
	dat, err := t.DB.Get(ro, b)
	if err != nil {
		return nil, err
	}
	res := new(Node)
	json.Unmarshal(dat, res)
	return res, nil

}

func (t *Tree) Put(wo *levigo.WriteOptions, n *Node) error {
	debug("Put")
	if dat, err := json.Marshal(n); err != nil {
		return err
	} else {
		b := int2b(n.Id)
		if err = t.DB.Put(wo, b, dat); err != nil {
			return err
		}
	}
	return nil
}

func NewTree(dbname string) *Tree {
	debug("New tree")
	n := new(Tree)
	n.Root = &Node{Id: 0}
	n.Newid = make(chan int)
	go func() {
		id := n.Root.Id + 1
		for ; ; id++ {
			n.Newid <- id
		}
	}()	
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(dbname, opts)
	if err != nil {
		log.Fatal(err)
	}
	n.DB = db
	wo := levigo.NewWriteOptions()
	if err := n.Put(wo, n.Root); err != nil {
		Error(err)
	}
	defer wo.Close()
	return n
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

func (t *Tree) fetchChild(ro *levigo.ReadOptions, n *Node, s string) *Node {
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
	ro := levigo.NewReadOptions()
	defer ro.Close()
	res, i := t.LookupOpt(ro, n, search, i)
	return res, i
}

func (t *Tree) LookupOpt(opt *levigo.ReadOptions, n *Node, search string, i int) (*Node, int) {

	if n == nil {
		return nil, i
	}
	debugf("Lookup(%+v, \"%s\", %d)\n", n, search, i)
	match := matchprefix(n.Edgename, search[i:])
	i += len(match)
	if i < len(search) && len(n.Edgename) == len(match) {
		child := t.fetchChild(opt, n, string(search[i]))
		c, i := t.LookupOpt(opt, child, search, i)
		if c != nil {
			return c, i
		}
	}
	return n, i
}

func (t *Tree) addChild(wo *levigo.WriteOptions, parent *Node, edgename, name string, value []string) error {
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
	wo := levigo.NewWriteOptions()
	ro := levigo.NewReadOptions()
	defer ro.Close()
	defer wo.Close()
	t.InsertOpt(wo, ro, k, v)
}

func (t *Tree) InsertOpt(wo *levigo.WriteOptions, ro *levigo.ReadOptions, k, v string) {
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

	// index of edgename
	ie := strings.LastIndex(n.Name, n.Edgename)
	midname := n.Name[ie:len(commonprefix)]

	debugf("left \"%s\" right \"%s\"\n", lname, rname)

	// left node (preserve the old string)
	leftnode := new(Node)
	leftnode.Value = n.Value
	leftnode.Edgename = lname
	leftnode.Name = mid.Name
	leftnode.Id = <-t.Newid
	leftnode.Children = mid.Children
	children[string(leftnode.Edgename[0])] = leftnode.Id

	// update the middle node
	mid.Edgename = midname
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
	midparent, err := t.Get(ro, n.Parent)
	if err != nil {
		Error(err)
	}
	midparent.Children[string(midname[0])] = mid.Id

	t.Put(wo, midparent)
	t.Put(wo, mid)
	t.Put(wo, leftnode)
}
