#  Extension of the xml.etree.ElementTree.Element object to enable access to both the current node and the immediate
#  parent node while walking (iterating) the subtree.  Returns a 2-tuple with current node and its parent as two
#  ElementTree.Element objects.  This was necessary solely because there are a few ambiguous tags in the Banking, Loan,
#  and Credit Card lists which must be disambiguated by adding its parent.  This was created as a module mainly to get
#  the messy details of iterating the subtree out of the main logic.

class ElandParent:

    def __init__(self, treeEl):
        self.__start = treeEl

    def __iter__(self):
        self.__PDL = [[self.__start]]
        return self

    def __next__(self):
        while len(self.__PDL)>0:
            if len(self.__PDL[-1])<=0:
                self.__PDL.pop(-1)      # Empty list on stack top-Pop it off, then pop off the leftmost, topmost entry
                if len(self.__PDL)>0 and len(self.__PDL[-1])>0:  self.__PDL[-1].pop(0)   # (Already processed)
            else:
                parent = self.__PDL[-2][0] if len(self.__PDL)>1 else None
                current = self.__PDL[-1][0]   # Process leftmost, topmost PDL entry (Leave on stack until later)
                x = current.findall('*')      # Then get all the direct children as a (possibly empty) list
                self.__PDL.append(x)          # and push the entire list onto the stack to be processed next in order
                return tuple((current, parent))
        raise StopIteration
