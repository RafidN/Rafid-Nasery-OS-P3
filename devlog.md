### Dec 2, 2025 7:16PM
#### First Look at the Project

From the initial read, I just need to make a command-line tool that manages the 
index file given read as a B-tree. The index file has a very specific layout I'll have
to follow. The program has create, insert, search, load, print, and extract commands. I 
also need to make sure that only 3 nodes are stored in memory at once.

I hope to finish this in one sitting !

#### Notes for planning etc.
Probably just going to do this all in 1 file for simplicity.
Had to do a similar assignment for Dr. Khan's advanced algorithms class, gonna
see if I can find that file, but that project was single values only.


### Dec 2, 2025 7:35PM
#### This sessions goals:
Complete this entire project so I can start studying for my final exams

#### Notes during development:

- For the menu, a simple switch case that routes to each function
  - if the number of arguments dont match expected, print out expected arguments for every function and exit
- To develop the data structure gonna implement Java closeable interface, as I used that in a previous course
- Designed the on-disk format by:
  - Block 0: magic, rootBlockId, nextBlockId, padded to 512 bytes
  - Each node block: blockId, parentBlockId, numKeys, fixed arrays of keys values and child block Ids 
- Setup B-Tree parameters according to requirements
  - min-degree of 10, max-keys 19, max-children 20. Each node has fixed-size arrays to these constants
- Node I/O just using Java's RandomAccessFile, maps blockId -> offset using blockId * BlockSize
  - Will always write full blocks to keep it consistent
- Insertion logic: (hardest part)
  - If tree is empty, set root and insert directly
  - If root is full, create new root and split old one, then insert to new root
  - Used iterative insertion to keep memory in control 
- SplitChild logic:
  - move upper half of keys/children to new node
  - promote the middle key
  - adjust numKeys and zero out unused entries
- Search logic:
  - walk down from rootBlockId, reads one node at a time and doing linear scan of the keys
- Print/Extract logic:
  - Just used BFS for the sake of storing minimal objects in memory
- Chose to allow duplicate keys, with no special handling
- Manually tested flows create -> insert -> search, load from CSV -> print, extract
- While testing I added 21:210 and it worked
- Copy and pasted the testing for all functionality into file "testing-terminal.txt" for viewing
  - The resulting .csv is named dump.csv
- THANK GOD WE DONT HAVE TO DO DELETE

### Dec 3, 2025 12:24PM

#### SESSION REFLECTION

Did I encounter any problems you have not already wrote about?
. Tried to log as much as I could about problems and my design decisions so no.

Did I accomplish my goal for this session?
- Yes the program is complete.