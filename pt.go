package pt

import (
_	"encoding/json"
_	"log"
	"runtime"
	"fmt"
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
	Key      string            `json:"key"`
	Value    []string          `json:"value"`
	Children map[string]string `json:"children"`
	Parent   string            `json:"parent"`
	Edgename string            `json:"edgename"`
}

/*
	Lookup takes the node to start from, the string to search for, and a
	count of how many chars are matched already.

	It returns the node that matches most closely and the number of
	characters (starting from 0) that match.

*/
func (t *DiskTree) Lookup(n *Node, search string, i int) (*Node, int) {

	if n == nil {
		return nil, i
	}
	dn := t.dnodeFromNode(n)
	debugf("Lookup(%+v, \"%s\", %d)\n", dn, search, i)
	match := matchprefix(n.Edgename, search[i:])
	i += len(match)
	if i < len(search) && len(n.Edgename) == len(match) {
		child := t.fetchChild(n, string(search[i]))
		c, i := t.Lookup(child, search, i)
		if c != nil {
			return c, i
		}
	}
	return n, i
}

func (t *DiskTree) Insert(k, v string) {
	debug("insert", k, v)
	root := t.root.toMem()
	n, i := t.Lookup(root, k, 0)
	commonprefix := k[:i]
	debug("Insert", k, "and commonprefix is", commonprefix)

	debugf("Lookup returns node '%+v' mathced chars = '%v' match '%v'\n", n, i, k[:i])

	debug("is it the root?")
	if n == root {
		debug("addChild")
		latestroot := t.dnodeFromHash(t.root.Hash)
		t.addChild(latestroot, k, k, []string{v})
		debug("yes")
		return
	}
	debug("no")

	debug("is it a complete match?")
	if k == n.Key {
		dn := t.dnodeFromNode(n)
		dn.Value = append(dn.Value, v)
		t.write(dn)

		debug("node", n, "already found append value here TODO")
		debug("yes")
		return
	}
	debug("no")

	debug("does commonprefix consume the whole tree so far?")
	// the best match matches the whole key (including n.Edgename)
	if commonprefix == n.Key {
		// but if it is longer than the key it's a simple add
		if len(k) > len(n.Key) {
			e := k[len(commonprefix):]
			dn := t.dnodeFromNode(n)
			t.addChild(dn, e, k, []string{v})
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

	mid := t.dnodeFromNode(n)
	children := make(map[string]string)

	// whatever is left in n.Key after taking out the length of common prefix
	lname := n.Key[len(commonprefix):]
	rname := k[len(commonprefix):]

	// index of edgename
	ie := strings.LastIndex(n.Key, n.Edgename)
	midname := n.Key[ie:len(commonprefix)]

	debug("into", lname, rname)

	// left node (preserve the old string)
	leftnode := new(disknode)
	leftnode.Value = n.Value
	leftnode.Edgename = lname
	leftnode.Key = mid.Key
	leftnode.Hash = mid.Hash
	leftnode.Children = mid.Children
	children[string(leftnode.Edgename[0])] = leftnode.Hash

	// update the middle node
	mid.Edgename = midname
	mid.Value = []string{}
	mid.Key = commonprefix
	mid.Hash = smash(commonprefix)
	leftnode.Parent = mid.Hash

	// if you have 'cats' and try to add 'cat'
	// you'll have this empty right node case
	if rname != "" {
		// right node (add the new string)
		rightnode := new(disknode)
		rightnode.Value = append(rightnode.Value, v)
		rightnode.Edgename = rname
		rightnode.Key = k
		rightnode.Hash = smash(k)
		children[string(rightnode.Edgename[0])] = rightnode.Hash
		rightnode.Parent = mid.Hash
		t.write(rightnode)
	}

	mid.Children = children

	// also update mid's parent hash
	midparent := t.dnodeFromHash(mid.Parent)
	midparent.Children[string(midname[0])] = mid.Hash

	t.write(midparent)
	t.write(mid)
	t.write(leftnode)

}



