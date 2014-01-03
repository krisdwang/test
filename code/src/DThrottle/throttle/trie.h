#ifndef _TRIE_H_
#define _TRIE_H_

#include <string>
#include <map>

/**
  A trie (from reTRIEval), is a multi-way tree structure useful for storing
  strings over an alphabet. The idea is that all strings sharing a common
  stem or prefix hang off a common node.

  template parameters:
    U = the type of userdata you wish to decorate trie nodes with
    T = the alphabet for strings (typically char)

  */
template<class U, class T = char>
class trie
{
public:
    typedef basic_string<T> string_t;

    /**
      Construct a new, empty trie
      */
    trie()
    {
	_root.NEW(new trie_node);
    }

    /**
      Inserts the string 's' into the trie along with 'userdata'
      */
    void insert(const string_t &s, const U &userdata)
    {
	trie_node *node = traverse(s, true, NULL);

	// if userdata decoration already exists, overwrite it
	if (node->data)
	{
	    delete node->data;
	    node->data = NULL;
	}
	node->data = new U(userdata);
    }

    /**
      Searches for the string 's' in the trie. 

      It returns a pointer to the 'userdata' which occurs first on a 
      trie traversal from the root to the last point at which the characters
      of s are found in the trie.
      
      If no such userdata exists, returns NULL.
      */
    U *lookup(const string_t &s)
    {
	U *retval = NULL;
	traverse(s, false, &retval);
	return retval;
    }

private:
    struct trie_node; // fwd

    struct trie_edge : public FLRetainable
    {
	T ch;			// character on this edge
	P<trie_node> next;	// pointer to the next node down

	trie_edge(T c, trie_node *n) : ch(c), next(n) {}
    };

    struct trie_node : public FLRetainable 
    {
	typedef map<T, P<trie_edge> > map_t;

	map_t children;	// map of characters to edges
	U *data; 	// userdata decoration 
			// not a smart pointer b/c U may not be FLRetainable

	trie_node() : data(NULL) {}

	~trie_node()
	{
	    if (data) 
	    {
		delete data;
		data = NULL;
	    }
	}
    };

    /**
      Traverse the trie from the root along a path defined by s.

      if create == true
	  nodes corresponding to characters of s will be created if necessary
	  (effectively adding s to the trie)
	  returns the node corresponding to the last character of s
      
      if create == false
	  returns the node at which s falls off the trie

      if data != NULL, upon return *data will return the lowest userdata
      decoration found during the traversal.
      */
    trie_node *traverse(const string_t &s, const bool create, U **data)
    {
	trie_node *p = _root;
	if (data) *data = p->data;

	// walk through the string, simultaneously stepping down the tree
	for (typename string_t::const_iterator i = s.begin(); i != s.end(); ++i)
	{
	    P<trie_edge> &edge = p->children[*i];
	    if (! edge)
	    {
		if (create)
		{
		    P<trie_node> new_node(NEW, new trie_node);
		    edge.NEW(new trie_edge(*i, new_node));
		}
		else 
		{
		    // fell off the trie!
		    return p;
		}
	    }

	    p = edge->next;

	    // keep track of the userdata decorations
	    if (data && p->data) *data = p->data;
	}
	return p;
    }

    P<trie_node> _root;		// the trie's root node
};

#endif // _TRIE_H_
