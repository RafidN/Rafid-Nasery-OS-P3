import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;

public class project3 {

    public static void main(String[] args) {
        if (args.length < 2) {
            usageAndExit();
        }

        String command = args[0];
        switch (command) {
            case "create":
                if (args.length != 2) usageAndExit();
                doCreate(args[1]);
                break;
            case "insert":
                if (args.length != 4) usageAndExit();
                doInsert(args[1], args[2], args[3]);
                break;
            case "search":
                if (args.length != 3) usageAndExit();
                doSearch(args[1], args[2]);
                break;
            case "load":
                if (args.length != 3) usageAndExit();
                doLoad(args[1], args[2]);
                break;
            case "print":
                if (args.length != 2) usageAndExit();
                doPrint(args[1]);
                break;
            case "extract":
                if (args.length != 3) usageAndExit();
                doExtract(args[1], args[2]);
                break;
            default:
                System.err.println("Unknown command: " + command);
                usageAndExit();
        }
    }

    private static void usageAndExit() {
        System.err.println("Usage:");
        System.err.println("  java project3 create <indexfile>");
        System.err.println("  java project3 insert <indexfile> <key> <value>");
        System.err.println("  java project3 search <indexfile> <key>");
        System.err.println("  java project3 load <indexfile> <csvfile>");
        System.err.println("  java project3 print <indexfile>");
        System.err.println("  java project3 extract <indexfile> <csvfile>");
        System.exit(1);
    }

    private static void doCreate(String indexFile) {
        File f = new File(indexFile);
        if (f.exists()) {
            System.err.println("Error: index file already exists.");
            System.exit(1);
        }
        try (BTreeIndex idx = BTreeIndex.create(indexFile)) {
        } catch (IOException e) {
            System.err.println("Error creating index file: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void doInsert(String indexFile, String keyStr, String valueStr) {
        long key;
        long value;
        try {
            key = Long.parseLong(keyStr);
            value = Long.parseLong(valueStr);
        } catch (NumberFormatException e) {
            System.err.println("Error: key and value must be integers.");
            System.exit(1);
            return;
        }

        try (BTreeIndex idx = BTreeIndex.open(indexFile)) {
            idx.insert(key, value);
        } catch (IOException e) {
            System.err.println("Error inserting into index: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void doSearch(String indexFile, String keyStr) {
        long key;
        try {
            key = Long.parseLong(keyStr);
        } catch (NumberFormatException e) {
            System.err.println("Error: key must be an integer.");
            System.exit(1);
            return;
        }

        try (BTreeIndex idx = BTreeIndex.open(indexFile)) {
            Long value = idx.search(key);
            if (value == null) {
                System.out.println("NOT FOUND");
            } else {
                System.out.println(key + " " + value);
            }
        } catch (IOException e) {
            System.err.println("Error searching index: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void doLoad(String indexFile, String csvFile) {
        File f = new File(csvFile);
        if (!f.exists()) {
            System.err.println("Error: CSV file does not exist.");
            System.exit(1);
        }

        try (BTreeIndex idx = BTreeIndex.open(indexFile);
             BufferedReader br = new BufferedReader(new FileReader(f))) {

            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split(",");
                if (parts.length != 2) {
                    System.err.println("Error: invalid CSV line: " + line);
                    System.exit(1);
                }
                long key = Long.parseLong(parts[0].trim());
                long value = Long.parseLong(parts[1].trim());
                idx.insert(key, value);
            }
        } catch (IOException | NumberFormatException e) {
            System.err.println("Error loading from CSV: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void doPrint(String indexFile) {
        try (BTreeIndex idx = BTreeIndex.open(indexFile)) {
            idx.printAll();
        } catch (IOException e) {
            System.err.println("Error printing index: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void doExtract(String indexFile, String csvFile) {
        File out = new File(csvFile);
        if (out.exists()) {
            System.err.println("Error: output CSV file already exists.");
            System.exit(1);
        }

        try (BTreeIndex idx = BTreeIndex.open(indexFile);
             BufferedWriter bw = new BufferedWriter(new FileWriter(out))) {
            idx.extractAll(bw);
        } catch (IOException e) {
            System.err.println("Error extracting index: " + e.getMessage());
            System.exit(1);
        }
    }

    private static class BTreeIndex implements Closeable {
        private static final int BLOCK_SIZE = 512;
        private static final int MIN_DEGREE = 10;
        private static final int MAX_KEYS = 2 * MIN_DEGREE - 1;
        private static final int MAX_CHILDREN = 2 * MIN_DEGREE;

        private static final String MAGIC_STRING = "4348PRJ3";

        private final RandomAccessFile raf;
        private long rootBlockId;
        private long nextBlockId;

        public static BTreeIndex create(String filename) throws IOException {
            RandomAccessFile raf = new RandomAccessFile(filename, "rw");
            BTreeIndex idx = new BTreeIndex(raf);
            idx.rootBlockId = 0;
            idx.nextBlockId = 1;
            idx.writeHeader();
            return idx;
        }

        public static BTreeIndex open(String filename) throws IOException {
            File f = new File(filename);
            if (!f.exists()) {
                System.err.println("Error: index file does not exist.");
                System.exit(1);
            }
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            BTreeIndex idx = new BTreeIndex(raf);
            idx.readHeader();
            return idx;
        }

        private BTreeIndex(RandomAccessFile raf) {
            this.raf = raf;
        }

        private void readHeader() throws IOException {
            if (raf.length() < BLOCK_SIZE) {
                throw new IOException("Invalid index file (too small).");
            }
            raf.seek(0);
            byte[] magicBytes = new byte[8];
            int read = raf.read(magicBytes);
            if (read != 8) {
                throw new IOException("Invalid index file (bad magic).");
            }
            String magic = new String(magicBytes, StandardCharsets.US_ASCII);
            if (!MAGIC_STRING.equals(magic)) {
                throw new IOException("Invalid index file (magic mismatch).");
            }
            rootBlockId = raf.readLong();
            nextBlockId = raf.readLong();
        }

        private void writeHeader() throws IOException {
            raf.seek(0);
            byte[] magicBytes = MAGIC_STRING.getBytes(StandardCharsets.US_ASCII);
            if (magicBytes.length != 8) {
                throw new IOException("MAGIC_STRING must be 8 bytes.");
            }
            raf.write(magicBytes);
            raf.writeLong(rootBlockId);
            raf.writeLong(nextBlockId);
            long pos = raf.getFilePointer();
            if (pos < BLOCK_SIZE) {
                int remaining = (int) (BLOCK_SIZE - pos);
                byte[] zeros = new byte[remaining];
                raf.write(zeros);
            }
        }

        @Override
        public void close() throws IOException {
            raf.close();
        }

        private static class Node {
            long blockId;
            long parentBlockId;
            int numKeys;
            long[] keys = new long[MAX_KEYS];
            long[] values = new long[MAX_KEYS];
            long[] children = new long[MAX_CHILDREN];

            boolean isLeaf() {
                for (int i = 0; i <= numKeys; i++) {
                    if (children[i] != 0) return false;
                }
                return true;
            }
        }

        private long offsetForBlock(long blockId) {
            return blockId * (long) BLOCK_SIZE;
        }

        private Node readNode(long blockId) throws IOException {
            if (blockId == 0) {
                throw new IllegalArgumentException("Attempt to read block 0 as node.");
            }
            Node node = new Node();
            node.blockId = blockId;

            raf.seek(offsetForBlock(blockId));
            long storedBlockId = raf.readLong();
            long parent = raf.readLong();
            long numKeysLong = raf.readLong();

            node.parentBlockId = parent;
            node.numKeys = (int) numKeysLong;

            for (int i = 0; i < MAX_KEYS; i++) {
                node.keys[i] = raf.readLong();
            }
            for (int i = 0; i < MAX_KEYS; i++) {
                node.values[i] = raf.readLong();
            }
            for (int i = 0; i < MAX_CHILDREN; i++) {
                node.children[i] = raf.readLong();
            }

            return node;
        }

        private void writeNode(Node node) throws IOException {
            if (node.blockId == 0) {
                throw new IllegalArgumentException("Node blockId cannot be 0.");
            }
            raf.seek(offsetForBlock(node.blockId));
            raf.writeLong(node.blockId);
            raf.writeLong(node.parentBlockId);
            raf.writeLong(node.numKeys);

            for (int i = 0; i < MAX_KEYS; i++) {
                raf.writeLong(node.keys[i]);
            }
            for (int i = 0; i < MAX_KEYS; i++) {
                raf.writeLong(node.values[i]);
            }
            for (int i = 0; i < MAX_CHILDREN; i++) {
                raf.writeLong(node.children[i]);
            }
        }

        private Node allocateEmptyNode() throws IOException {
            long blockId = nextBlockId;
            nextBlockId++;
            writeHeader();
            Node node = new Node();
            node.blockId = blockId;
            node.parentBlockId = 0;
            node.numKeys = 0;
            return node;
        }

        public void insert(long key, long value) throws IOException {
            if (rootBlockId == 0) {
                Node root = allocateEmptyNode();
                root.numKeys = 1;
                root.keys[0] = key;
                root.values[0] = value;
                writeNode(root);
                rootBlockId = root.blockId;
                writeHeader();
                return;
            }

            Node root = readNode(rootBlockId);
            if (root.numKeys == MAX_KEYS) {
                Node newRoot = allocateEmptyNode();
                newRoot.parentBlockId = 0;
                newRoot.numKeys = 0;
                newRoot.children[0] = root.blockId;

                root.parentBlockId = newRoot.blockId;
                writeNode(root);

                splitChild(newRoot, 0, root);
                writeNode(newRoot);

                rootBlockId = newRoot.blockId;
                writeHeader();

                insertNonFullIterative(newRoot, key, value);
            } else {
                insertNonFullIterative(root, key, value);
            }
        }

        private void insertNonFullIterative(Node node, long key, long value) throws IOException {
            Node current = node;

            while (true) {
                if (current.isLeaf()) {
                    insertIntoLeaf(current, key, value);
                    writeNode(current);
                    return;
                } else {
                    int i = 0;
                    while (i < current.numKeys && key > current.keys[i]) {
                        i++;
                    }
                    long childBlockId = current.children[i];
                    if (childBlockId == 0) {
                        insertIntoLeaf(current, key, value);
                        writeNode(current);
                        return;
                    }

                    Node child = readNode(childBlockId);
                    if (child.numKeys == MAX_KEYS) {
                        splitChild(current, i, child);
                        writeNode(current);
                        if (key > current.keys[i]) {
                            i++;
                        }
                        childBlockId = current.children[i];
                        child = readNode(childBlockId);
                    }

                    current = child;
                }
            }
        }

        private void insertIntoLeaf(Node node, long key, long value) {
            int i = node.numKeys - 1;
            while (i >= 0 && key < node.keys[i]) {
                node.keys[i + 1] = node.keys[i];
                node.values[i + 1] = node.values[i];
                i--;
            }
            node.keys[i + 1] = key;
            node.values[i + 1] = value;
            node.numKeys++;
        }

        private void splitChild(Node parent, int childIndex, Node child) throws IOException {
            Node newNode = allocateEmptyNode();
            newNode.parentBlockId = parent.blockId;

            int t = MIN_DEGREE;

            newNode.numKeys = t - 1;
            for (int j = 0; j < t - 1; j++) {
                newNode.keys[j] = child.keys[j + t];
                newNode.values[j] = child.values[j + t];
            }

            if (!child.isLeaf()) {
                for (int j = 0; j < t; j++) {
                    newNode.children[j] = child.children[j + t];
                }
                for (int j = t; j < MAX_CHILDREN; j++) {
                    newNode.children[j] = 0;
                }
            }

            int oldNumKeys = child.numKeys;
            child.numKeys = t - 1;

            for (int j = t - 1; j < MAX_KEYS; j++) {
                if (j >= child.numKeys) {
                    child.keys[j] = 0;
                    child.values[j] = 0;
                }
            }
            if (!child.isLeaf()) {
                for (int j = t; j < MAX_CHILDREN; j++) {
                    child.children[j] = 0;
                }
            }

            for (int j = parent.numKeys; j >= childIndex + 1; j--) {
                parent.children[j + 1] = parent.children[j];
            }
            parent.children[childIndex + 1] = newNode.blockId;

            for (int j = parent.numKeys - 1; j >= childIndex; j--) {
                parent.keys[j + 1] = parent.keys[j];
                parent.values[j + 1] = parent.values[j];
            }
            parent.keys[childIndex] = child.keys[t - 1];
            parent.values[childIndex] = child.values[t - 1];
            parent.numKeys++;

            child.keys[t - 1] = 0;
            child.values[t - 1] = 0;

            writeNode(child);
            writeNode(newNode);
        }

        public Long search(long key) throws IOException {
            if (rootBlockId == 0) return null;

            long currentBlockId = rootBlockId;

            while (currentBlockId != 0) {
                Node node = readNode(currentBlockId);
                int i = 0;
                while (i < node.numKeys && key > node.keys[i]) {
                    i++;
                }
                if (i < node.numKeys && key == node.keys[i]) {
                    return node.values[i];
                } else {
                    if (node.children[i] == 0) {
                        return null;
                    } else {
                        currentBlockId = node.children[i];
                    }
                }
            }
            return null;
        }

        public void printAll() throws IOException {
            if (rootBlockId == 0) {
                return;
            }
            Deque<Long> queue = new ArrayDeque<>();
            queue.add(rootBlockId);

            while (!queue.isEmpty()) {
                long blockId = queue.removeFirst();
                Node node = readNode(blockId);

                for (int i = 0; i < node.numKeys; i++) {
                    System.out.println(node.keys[i] + " " + node.values[i]);
                }

                for (int i = 0; i <= node.numKeys; i++) {
                    long child = node.children[i];
                    if (child != 0) {
                        queue.addLast(child);
                    }
                }
            }
        }

        public void extractAll(BufferedWriter bw) throws IOException {
            if (rootBlockId == 0) {
                return;
            }
            Deque<Long> queue = new ArrayDeque<>();
            queue.add(rootBlockId);

            while (!queue.isEmpty()) {
                long blockId = queue.removeFirst();
                Node node = readNode(blockId);

                for (int i = 0; i < node.numKeys; i++) {
                    bw.write(node.keys[i] + "," + node.values[i]);
                    bw.newLine();
                }

                for (int i = 0; i <= node.numKeys; i++) {
                    long child = node.children[i];
                    if (child != 0) {
                        queue.addLast(child);
                    }
                }
            }
            bw.flush();
        }
    }
}
