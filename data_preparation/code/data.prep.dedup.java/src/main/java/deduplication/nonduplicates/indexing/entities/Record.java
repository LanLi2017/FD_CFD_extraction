package deduplication.nonduplicates.indexing.entities;

import java.io.Serializable;
import java.util.*;

/**
 * Created by koumarelas on 9/6/16.
 */
public class Record implements Comparable<Record>, Serializable {

    protected Map<String, String> values;

    public Record(Record r) {
        this.values = new HashMap<>(r.values);
    }

    public Record() {
        values = new HashMap<>();
    }

    public Record(Map<String, String> values) {
        this.values = new HashMap<>();
        if (values != null) {
            for (Map.Entry<String, String> entry : values.entrySet()) {
                setAttribute(entry.getKey(), entry.getValue());
            }

            if (!values.containsKey("normalized")) {
                this.values.put("normalized", "false");
            }
        }
    }

    public String getAttribute(String attributeName) {
        return values.getOrDefault(attributeName, "");
    }

    public void setAttribute(String attributeName, String value) {
        // WARNING: For space reasons, we don't actually save empty selected_attributes/values.

        if (value != null && !value.trim().equals("")) {
            values.put(attributeName, value);
        } else if (values.containsKey(attributeName)) {
            values.remove(attributeName);
        }
    }

    public Boolean containsAttribute(String attributeName) {
        return values.containsKey(attributeName);
    }

    public Map<String, String> getValues() {
        return values;
    }

    public void setValues(Map<String, String> values) {
        this.values = values;
    }

    public void removeAttribute (String attribute) {
        values.remove(attribute);
    }

    @Override
    public String toString() {
        List<String> vls = new ArrayList<>();
        values.keySet().forEach(x -> vls.add(x + "=" + values.getOrDefault(x, "")));
        return String.join("\t", vls);
    }

    public String getID() {
        return values.get("id");
    }

    @Override
    public int compareTo(Record r) {
        Set<String> attributes = new HashSet<>(this.values.keySet());
        attributes.addAll(r.values.keySet());
        for (String attr : attributes) {
            Integer cmp = values.get(attr).compareTo(r.values.get(attr));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }
}
