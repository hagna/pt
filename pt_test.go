package pt

import (
	"github.com/jmhodges/levigo"
	"os"
	"strings"
	"testing"
	"fmt"
	"bytes"
)

func TestCrud(t *testing.T) {
	dirname := "root1"
	wo := levigo.NewWriteOptions()
	ro := levigo.NewReadOptions()
	defer ro.Close()
	defer wo.Close()
	defer os.RemoveAll(dirname)
	l := NewTree(dirname)
	n := &Node{Name: "foo", Id: 1}
	l.Put(wo, n)
	z, _ := l.Get(ro, 1)
	if z.Name != n.Name {
		t.Fatal("Didn't get what I put in", z.Name, n.Name)
	}
	n2 := &Node{Name: "bar", Id: 1}
	l.Put(wo, n2)
	z, _ = l.Get(ro, 1)
	if z.Name != n2.Name {
		t.Fatal("Didn't get updated", z.Name, n2.Name)
	}
}

func isFound(t *testing.T, tree *Tree, s string, v string) {
	a, i := tree.Lookup(tree.Root, s, 0)
	if i != len(a.Name) || a.Name != s {
		t.Fatalf("could not find string %s closest was %+v\n", s, a)
	}
	foundval := false
	for _, val := range a.Value {
		if v == val {
			foundval = true
		}
	}
	if !foundval {
		t.Fatalf("could not find value %s in %+v\n", v, a)
	}
}

func TestInsertLookup(t *testing.T) {
	l := `back
abstruse
abstracts
abstractions
abstraction
abstracted
abstract
abstinent
abstinence
abstentions
abstention
abstaining
abstained
abstain
abating
abalone
abacus
Ab
Ab
aarons
Aaron
aaron
aaron
aardvark
aardvark
aaa
a`
	value := 0
	s := strings.Split(l, "\n")
	dirname := "root2"
	defer os.RemoveAll(dirname)
	tree := NewTree(dirname)
	for _, v := range s {
		tree.Insert(v, fmt.Sprintf("%d", value))
		value++
	}
	value = 0
	for _, v := range s {
		isFound(t, tree, v, fmt.Sprintf("%d", value))
		value++
	}
	b := bytes.NewBuffer([]byte{})
	tree.Print(b, tree.Root, "")
	t.Log(b.String()) 
	tree.Close()
}
