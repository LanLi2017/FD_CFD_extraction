package deduplication.classification.utils;

import java.util.Comparator;

public class DedupPair implements Comparable<DedupPair>{
    protected String dataset = null;
    protected String id;
    protected String id1 = null;
    protected String id2 = null;
    protected PairClass pairClass = null;

    @Override
    public int compareTo(DedupPair o) {
        if (this == o) {
            return 0;
        }
//        if (!(o instanceof DedupPair))
//            return 0;
//        else {
        Comparator<DedupPair> compare = Comparator.comparing(DedupPair::getDataset)
                .thenComparing(DedupPair::getId1)
                .thenComparing(DedupPair::getId2)
                .thenComparing(DedupPair::getPairClass);
        return compare.compare(this, (DedupPair)o);
//        }
    }

    @Override
    public String toString() {
        return id1 + "_" + id2;
    }

    @Override
    public int hashCode() {
        int hashcode = dataset.hashCode();
        hashcode *= id1.hashCode();
        hashcode *= id2.hashCode();
        return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DedupPair)) {
            return false;
        } else {
            DedupPair o = (DedupPair) obj;
            return (this.dataset.equals(o.dataset) && this.id1.equals(o.id1) && this.id2.equals(o.id2) &&
                    this.pairClass.ordinal() == o.pairClass.ordinal());
        }
    }

    public static enum PairClass {
        NDPL, DPL, UNKNOWN
    }


    public DedupPair(String dataset, String id, String id1, String id2, PairClass pairClass) {
        this.dataset = dataset;
        this.id = id;
        this.id1 = id1;
        this.id2 = id2;
        this.pairClass = pairClass;
    }

    @Override
    public DedupPair clone() {
        return new DedupPair(dataset, id, id1, id2, pairClass);
    }


    public String getDataset() {
        return dataset;
    }

    public String getId() {
        return id;
    }

    public String getId1() {
        return id1;
    }

    public String getId2() {
        return id2;
    }

    public PairClass getPairClass() {
        return pairClass;
    }

    public void setPairClass(PairClass pairClass) {
        this.pairClass = pairClass;
    }
}
