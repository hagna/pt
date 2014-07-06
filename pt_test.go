package pt

import (
	"testing"
	"os"
	"github.com/jmhodges/levigo"
	"strings"
)

func TestCrud(t *testing.T) {
	dirname := "root1"
	wo := levigo.NewWriteOptions()
	ro := levigo.NewReadOptions()
	defer ro.Close()
	defer wo.Close()
	defer os.RemoveAll(dirname)
	l := NewTree(dirname)
	n := Node{Name: "foo", Id:1}
	l.Put(wo, n)
	z, _ := l.Get(ro, 1)
	if z.Name != n.Name {
		t.Fatal("Didn't get what I put in", z.Name, n.Name)
	}
	n2 := Node{Name: "bar", Id: 1}
	l.Put(wo, n2)
	z, _ = l.Get(ro, 1)
	if z.Name != n2.Name {
		t.Fatal("Didn't get updated", z.Name, n2.Name)
	}
}

func isFound(t *testing.T, tree *Tree, s string) {
	a, i := tree.Lookup(Root, s, 0)
	if i != len(a.Name) || a.Name != s {
		t.Fatalf("could not find string %s closest was %+v\n", s, a)
	}
}

func TestInsertLookup(t *testing.T) {
	l := `abating
abalone
abacus
aback
Ab
Ab
Aaron
aardvark`
	s := strings.Split(l, "\n")
	dirname := "root2"
	defer os.RemoveAll(dirname)
	tree := NewTree(dirname)
	for _, v := range s {
		tree.Insert(v, "")
	}
	for _, v := range s {
		isFound(t, tree, v)
	}
}


