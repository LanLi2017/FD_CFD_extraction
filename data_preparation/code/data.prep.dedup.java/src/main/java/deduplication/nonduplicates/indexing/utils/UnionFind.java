package deduplication.nonduplicates.indexing.utils;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Based on the implementation by
 * <a href="http://algs4.cs.princeton.edu/15uf/UF.java.html">Robert Sedgewick
 * and Kevin Wayne</a> from Princeton University
 *
 * <p>
 * For additional documentation, see
 * <a href="http://algs4.cs.princeton.edu/15uf">Section 1.5</a> of
 * <i>Algorithms, 4th Edition</i> by Robert Sedgewick and Kevin Wayne.
 * </p>
 *
 * @param <T>
 *            moduleType of the elements
 */
public class UnionFind<T> implements Iterable<Set<T>>, Serializable {
    private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(UnionFind.class.getName());

	private String indexId;
	private File indexDir;
	private File ufFile;

	public UnionFind() throws IOException {
		this(null, null, true);
	}

	public UnionFind(String indexId, File indexDir, Boolean purge) throws IOException {
		this.count = 0;
		this.indexId = indexId;
		this.indexDir = indexDir;
		this.ufFile = new File(indexDir + File.separator + "uf.bin");

        if (purge && indexDir != null && indexDir.isDirectory()) {
            LOGGER.log(Level.INFO, "Deleting IRMFramework directory: " + indexDir);
            FileUtils.deleteDirectory(indexDir);
        }

        if (indexDir != null && !indexDir.isDirectory()) {
            indexDir.mkdirs();
        }

        // Import nodes -- Map.
        if (indexDir != null) {
            if (ufFile.exists()) {
                try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(ufFile))) {
                    Object o = in.readObject();
                    nodes.putAll(o == null ? Collections.emptyMap() : (HashMap) o);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    LOGGER.log(Level.SEVERE, "There was a problem opening file: " + ufFile);
                }

                // Properties
                Properties propsInfo = new Properties();

                File propertiesFile = new File(indexDir + File.separator + "uf.properties");
                FileInputStream in = new FileInputStream(propertiesFile);
                propsInfo.load(in);
                in.close();

                count = Integer.parseInt(propsInfo.getProperty("count"));
                this.indexId = propsInfo.getProperty("indexId");
            }
		}
    }

    public void close() throws IOException {
		try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(ufFile))) {
			out.writeObject(nodes);
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.log(Level.SEVERE, "There was a problem saving file: " + ufFile);
		}

        File propertiesFile = new File(indexDir + File.separator + "uf.properties");
        propertiesFile.delete();
        Properties propsInfo = new Properties();

        propsInfo.setProperty("count", String.valueOf(count));
        propsInfo.setProperty("indexId", indexId);

        FileOutputStream out = new FileOutputStream(propertiesFile);
        propsInfo.store(out, "---No Comment---");
        out.close();
    }

	/**
	 * This class represents a node in the Union Find tree having a parent,
	 * multiple children and containing an element of moduleType {@code T}
	 *
	 */
	private static class Node<U> implements Serializable {

		private final Collection<Node<U>> children = new HashSet<>();
		private final U element;
		private Node<U> parent;
		private byte rank;

		/**
		 * Constructs a new node containing the specified element.
		 *
		 * @param element
		 *            element associated with this node
		 */
		public Node(U element) {
			this.element = element;
		}

		/**
		 * Adds a new child node to this node.
		 *
		 * @param child
		 *            new child node
		 */
		private void addChild(Node<U> child) {
			children.add(child);
		}

		public Node<U> find() {
			Node<U> node = this;
			Node<U> parent;
			while ((parent = node.getParent()) != null) {
				// path compression by halving
				Node<U> pp = parent.getParent();
				if (pp != null) {
					node.setParent(pp);
					node = pp;
				} else {
					return parent;
				}
			}
			return node;
		}

		/**
		 * Get all child nodes contained by this node.
		 *
		 * @return collection of child nodes
		 */
		public Collection<Node<U>> getChildren() {
			return children;
		}

		public Set<U> getComponent() {
			Set<U> component = new HashSet<>();
			Stack<Node<U>> todo = new Stack<>();
			todo.add(this);
			while (!todo.isEmpty()) {
				Node<U> elem = todo.pop();
				component.add(elem.getElement());
				todo.addAll(elem.getChildren());
			}
			return component;
		}

		/**
		 * Get the element contained by this node.
		 *
		 * @return element contained by this node
		 */
		public U getElement() {
			return element;
		}

		/**
		 * Get the parent node of this node.
		 *
		 * @return parent node of this node
		 */
		public Node<U> getParent() {
			return parent;
		}

		/**
		 * Get the rank of this node. The rank is increased whenever two nodes
		 * of same rank are unified. Hence, the rank grows logarithmic and
		 * should never exceed 31.
		 *
		 * @return rank of this node in the tree
		 */
		public byte getRank() {
			return rank;
		}

		/**
		 * Increases rank of this node. The rank is increased whenever two nodes
		 * of same rank are unified. Hence, the rank grows logarithmic and
		 * should never exceed 31.
		 */
		public void increaseRank() {
			rank++;
		}

		/**
		 * Removes the specified child from this node.
		 *
		 * @param child
		 *            the child to be removed
		 */
		public void removeChild(Node<U> child) {
			children.remove(child);
		}

		/**
		 * Sets the parent node of this node and updates the child relations
		 * accordingly.
		 *
		 * @param parent
		 *            the new parent node
		 */
		public void setParent(Node<U> parent) {
			if (this.parent != null) {
				this.parent.removeChild(this);
			}
			this.parent = parent;
			parent.addChild(this);
		}
	}

	/** Nodes rooting a tree */
	private int count = 0;
	/** Element-node mapping to retrieve the node */
	private final Map<T, Node<T>> nodes = new HashMap<>();

	/**
	 * Returns true if the the two sites are in the same component.
	 *
	 * @param t
	 *            the element representing one site
	 * @param u
	 *            the element representing the other site
	 * @return <tt>true</tt> if the two sites <tt>t</tt> and <tt>u</tt> are in
	 *         the same component; <tt>false</tt> otherwise
	 */
	public boolean connected(T t, T u) {
		Node<T> nodeT = findNode(t);
		Node<T> nodeU = findNode(u);
		return !(nodeT == null || nodeU == null) && nodeT.equals(nodeU);
	}

	/**
	 * Returns the number of disjoint components.
	 *
	 * @return the number of components
	 */
	public int count() {
		return count;
	}

	/**
	 * Returns the component identifier for the component containing site
	 * <tt>t</tt>.
	 *
	 * @param t
	 *            the element representing one site
	 * @return the component identifier for the component containing site
	 *         <tt>t</tt>
	 */
	public Node<T> findNode(T t) {
		if (t == null) {
			throw new NullPointerException("Element must not be null");
		}
		Node<T> node = nodes.get(t);
		if (node == null) {
			return null;
		}
		return node.find();
	}

	public T find(T t) {
	    return findNode(t).element;
    }

	/**
	 * Returns the elements contained in the same component as <tt>t</tt>
	 *
	 * @param t
	 *            the element representing one site
	 * @return the elements contained in the same component as <tt>t</tt>
	 *         excluding <tt>t</tt>
	 */
	public Collection<T> getComponent(T t) {
		Node<T> node = findNode(t);
		if (node == null) {
			return new HashSet<>();
		}
		return node.getComponent();
	}

	public Collection<Collection<T>> getComponents(Collection<T> t) {
        return    t.stream().map(ch -> findNode(ch).element).collect(Collectors.toSet()).  // children --> parents
                    stream().map(prnt -> getComponent(prnt)).collect(Collectors.toSet()); // parents --> components
	}

	/**
	 * Add a new element to the Union findNode by creating a new tree having exactly
	 * one node.
	 *
	 * @param t
	 *            element to be inserted
	 */
	private Node<T> insert(T t) {
		if (t == null) {
			throw new NullPointerException("Element must not be null");
		}
		Node<T> node = new Node<>(t);
		count++;
		nodes.put(t, node);
		return node;
	}

	@Override
	public Iterator<Set<T>> iterator() {
		return nodes.values()
				.stream()
				.map(n -> Pair.of(n.find(), n.getElement()))  // <root, node>
                .collect(
                        Collectors.groupingBy(Pair::getLeft, Collectors  // Group by root
                                .reducing(  Collections.<T> emptySet(),
                                            p -> Collections.singleton(p.getRight()),
                                            Sets::union)))
				.values().iterator();
	}

    public Collection<Set<T>> getComponentsParallel() {
        return nodes.values()
//                .stream()
                .parallelStream()
                .map(n -> Pair.of(n.find(), n.getElement()))  // <root, node>
                .collect(
                        Collectors.groupingBy(Pair::getLeft, Collectors  // Group by root
                                .reducing(  Collections.<T> emptySet(),
                                        p -> Collections.singleton(p.getRight()),
                                        Sets::union)))
                .values();
    }

	/**
	 * Merges the component containing site <tt>t</tt> with the the component
	 * containing site <tt>u</tt>.
	 *
	 * @param t
	 *            the element representing one site
	 * @param u
	 *            the element representing the other site
	 */
	public void union(T t, T u) {
		Node<T> nodeT = nodes.get(t);
		if (nodeT == null) {
			nodeT = insert(t);
		}
		Node<T> nodeU = nodes.get(u);
		if (nodeU == null) {
			nodeU = insert(u);
		}
		Node<T> rootT = nodeT.find();
		Node<T> rootU = nodeU.find();
		if (rootT.equals(rootU)) {
			return;
		}
		// make root of smaller rank point to root of larger rank
		count--;
		if (rootT.getRank() < rootU.getRank()) {
			rootT.setParent(rootU);
		} else if (rootT.getRank() > rootU.getRank()) {
			rootU.setParent(rootT);
		} else {
			rootU.setParent(rootT);
			rootT.increaseRank();
		}
	}
}