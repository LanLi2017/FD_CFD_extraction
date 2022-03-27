package deduplication.preparation.utils;

import java.util.Comparator;

public class Preparation implements Comparable<Preparation>{
    protected String dataset;
    protected String preparator;
    protected String attribute;
    protected Integer order;
    protected Boolean deduplicationSpecific;

    public Preparation(String dataset, String preparator, String attribute, Integer order, Boolean deduplicationSpecific) {
        this.dataset = dataset;
        this.preparator = preparator;
        this.attribute = attribute;
        this.order = order;
        this.deduplicationSpecific = deduplicationSpecific;
    }

    public String getDataset() {
        return dataset;
    }

    public String getPreparator() {
        return preparator;
    }

    public String getAttribute() {
        return attribute;
    }

    public Integer getOrder() {
        return order;
    }

    public Boolean getDeduplicationSpecific() {
        return deduplicationSpecific;
    }

    @Override
    public int compareTo(Preparation o) {
        Comparator<Preparation> compareByFirstName = Comparator
                .comparing(Preparation::getOrder);
        return compareByFirstName.compare(this, o);
    }
}
