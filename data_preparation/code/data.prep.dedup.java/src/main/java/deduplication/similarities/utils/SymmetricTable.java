package deduplication.similarities.utils;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;

import java.util.Set;

/**
 * Two-dimensional symmetric value store. Values are stored using two keys but
 * are retrieved using the keys in no matter which orders
 *
 * @param <K>
 *            type of the keys
 * @param <V>
 *            type of the values
 */
public class SymmetricTable<K extends Comparable<K>, V> {

	private final Table<K, K, V> table = HashBasedTable.create();

	/**
	 * Get all stored key-key-value triplets
	 *
	 * @return set of key-key-value triplets
	 */
	public Set<Cell<K, K, V>> cellSet() {
		return table.cellSet();
	}

	/**
	 * Check whether a value is associated with the two keys
	 *
	 * @param key1
	 *            first key
	 * @param key2
	 *            second key
	 * @return true if the table contains a value associated with the two keys
	 */
	public boolean contains(K key1, K key2) {
		return key1.compareTo(key2) < 0 ? table.contains(key1, key2): table.contains(key2, key1);
	}

	/**
	 * Retrieve the value associated with the two keys
	 *
	 * @param key1
	 *            first key
	 * @param key2
	 *            second key
	 * @return value associated with two keys, null if no value is stored with
	 *         those keys
	 */
	public V get(K key1, K key2) {
		if (key1.compareTo(key2) < 0) {
			if (table.contains(key1, key2)) {
				return table.get(key1, key2);
			}
		} else {
			if (table.contains(key2, key1)) {
				return table.get(key2, key1);
			}
		}
		return null;
	}

	/**
	 * Store a value with the specified keys
	 *
	 * @param key1
	 *            first key
	 * @param key2
	 *            second key
	 * @param value
	 *            value to be stored
	 */
	public void put(K key1, K key2, V value) {
		if (key1 != null && key2 != null) {
            if (key1.compareTo(key2) < 0) {
                table.put(key1, key2, value);
            } else {
                table.put(key2, key1, value);
            }
        }
	}

	public void retainAll(SymmetricTable st) {
        table.cellSet().retainAll(st.cellSet());
    }

    public void clear() {
	    table.clear();
    }

    public Set<K> getColumn(K k) {
	    return table.column(k).keySet();
    }

    public Set<K> getRow(K k) {
        return table.column(k).keySet();
    }

    public Integer getNumberColumns() {
	    return table.columnKeySet().size();
    }

    public Integer getNumberRows() {
        return table.rowKeySet().size();
    }
}
