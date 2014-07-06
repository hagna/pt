package pt

import (
	"encoding/json"
	"log"
	"runtime"
	"fmt"
	"strings"
	"github.com/jmhodges/levigo"
	"encoding/binary"
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
	Id	int	`json:"id"`
	Name      string            `json:"name"`
	Value    []string          `json:"value"`
	Children map[string]int `json:"children"`
	Parent   int `json:"parent"`
	Edgename string            `json:"edgename"`
}

var Root *Node = &Node{Id: 1}

type Tree struct {
	*levigo.DB
}

func int2b(a int) []byte {
	b := []byte{0,0,0,0}
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

func (t *Tree) Put(wo *levigo.WriteOptions, n Node) error {
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
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3<<30))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(dbname, opts)
	if err != nil {
		log.Fatal(err)
	}
	n.DB = db
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
	newid := n.Children[s]
	n, err := t.Get(ro, newid)
	if err != nil {
		log.Fatal(err)
	}
	return n
}

/*
	Lookup takes the node to start from, the string to search for, and a
	count of how many chars are matched already.

	It returns the node that matches most closely and the number of
	characters (starting from 0) that match.

*/
func (t* Tree) Lookup(n *Node, search string, i int) (*Node, int) {
	ro := levigo.NewReadOptions()
	defer ro.Close()
	res, i := t.OptLookup(ro, n, search, i)
	return res, i
}

func (t *Tree) OptLookup(opt *levigo.ReadOptions, n *Node, search string, i int) (*Node, int) {

	if n == nil {
		return nil, i
	}
	debugf("Lookup(%+v, \"%s\", %d)\n", n, search, i)
	match := matchprefix(n.Edgename, search[i:])
	i += len(match)
	if i < len(search) && len(n.Edgename) == len(match) {
		child := t.fetchChild(opt, n, string(search[i]))
		c, i := t.OptLookup(opt, child, search, i)
		if c != nil {
			return c, i
		}
	}
	return n, i
}

func (t *Tree) Insert(k, v string) {
	debug("insert", k, v)
	root := new(Node)
	n, i := t.Lookup(root, k, 0)
	commonprefix := k[:i]
	debug("Insert", k, "and commonprefix is", commonprefix)

	debugf("Lookup returns node '%+v' mathced chars = '%v' match '%v'\n", n, i, k[:i])

	debug("is it the root?")
	if n == root {
		debug("addChild")
		latestroot := root
		log.Println(latestroot)
		// t.addChild(latestroot, k, k, []string{v})
		debug("yes")
		return
	}
	debug("no")

	debug("is it a complete match?")
	if k == n.Name {
		dn := n
		dn.Value = append(dn.Value, v)
		// t.write(dn)

		debug("node", n, "already found append value here TODO")
		debug("yes")
		return
	}
	debug("no")

	debug("does commonprefix consume the whole tree so far?")
	// the best match matches the whole key (including n.Edgename)
	if commonprefix == n.Name {
		// but if it is longer than the key it's a simple add
		if len(k) > len(n.Name) {
			e := k[len(commonprefix):]
			log.Println(e)
			// t.addChild(dn, e, k, []string{v})
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

	debug("into", lname, rname)

	// left node (preserve the old string)
	leftnode := new(Node)
	leftnode.Value = n.Value
	leftnode.Edgename = lname
	leftnode.Name = mid.Name
	leftnode.Id = mid.Id
	leftnode.Children = mid.Children
	children[string(leftnode.Edgename[0])] = leftnode.Id

	// update the middle node
	mid.Edgename = midname
	mid.Value = []string{}
	mid.Name = commonprefix
	leftnode.Parent = mid.Id

	// if you have 'cats' and try to add 'cat'
	// you'll have this empty right node case
	if rname != "" {
		// right node (add the new string)
		rightnode := new(Node)
		rightnode.Value = append(rightnode.Value, v)
		rightnode.Edgename = rname
		rightnode.Name = k
		children[string(rightnode.Edgename[0])] = rightnode.Id
		rightnode.Parent = mid.Id
//		t.write(rightnode)
	}

	mid.Children = children

	// also update mid's parent id 
	midparent := n
	midparent.Children[string(midname[0])] = mid.Id

/*	t.write(midparent)
	t.write(mid)
	t.write(leftnode)
	*/

}



