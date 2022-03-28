package deduplication.similarities.utils.string.hybrid.utils;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.Serializable;

/**
 * Created by jokoum on 11/23/16.
 */
public class Commons {

    /* TokenType could be Comparable if we have many types */
    public static enum TokenType implements Serializable {
        TEXT(1.0), NUMBER(1.0);

        private Double weight;

        TokenType(Double weight) {
            this.weight = weight;
        }

        public Double getWeight() {
            return weight;
        }
    }
    public static TokenType getTokenType(String v) {
        if (NumberUtils.isNumber(v)) {
            return TokenType.NUMBER;
        } else {
            return TokenType.TEXT;
        }
    }

    public static enum MergingType implements Serializable {
        // We only execute once, caring for the string with the less tokens
        //   (to have stable matching, in the case of StableMatching)
        ONCE_SMALLER_BIGGER,

        // We keep all similarities, like executing two times aiming for good (or stable) matching for the one or the latter.
        BOTH_ALL,

        // We execute two times and take the average
        BOTH_ALL_AVERAGE,

        // We keep all the different matches. If i with j were matched, both when getting the good (stable) matching
        // for first string and for the second string, there is no reason to count it again.
        BOTH_UNION,

        // We keep the same matches: If i with j were matched both when (stable) matching for first string and
        //  for the second string.
        BOTH_INTERSECTION,

        // UNION - INTERSECTION
        BOTH_DIFFERENCE
    }

}
