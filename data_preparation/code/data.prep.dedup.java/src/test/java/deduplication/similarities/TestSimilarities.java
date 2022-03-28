package deduplication.similarities;

import deduplication.similarities.utils.string.edit.Levenshtein;
import deduplication.similarities.utils.string.hybrid.MongeElkan;
import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestSimilarities {

    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        MongeElkan.MongeElkanLevenshtein sim = new MongeElkan.MongeElkanLevenshtein("mel", Boolean.FALSE, Collections.emptyMap());
//        Levenshtein sim = new Levenshtein("mel", Boolean.FALSE, Collections.emptyMap());

        String v1, v2;

        List<Pair<String, String>> pairs = Arrays.asList(
            Pair.with("804 northpoint", "804 north point st."),
            Pair.with("804 north point", "804 north point st."),

            Pair.with("5937 geary blvd.", "14928 ventura blvd."),
            Pair.with("5937 geary blvd.", "14928 ven tu ra blvd."),

            Pair.with("L'Intrus", "Intrus, L'"),
            Pair.with("LIntrus", "Intrus L"),

            Pair.with("Oh... Rosalinda!!", "H.M.S. Defiant"),
            Pair.with("Oh Rosalinda", "HMS Defiant"),

            Pair.with("", "CHICAGO"),
            Pair.with("CHICAGO", "CHICAGO"),

            Pair.with("", "WILMINGTON"),
            Pair.with("HERNDON", "WILMINGTON"),

            Pair.with("Aha, D., Kibler, D., & Albert, M", "D. Aha, D. Kibler, and M. Albert."),
            Pair.with("Aha D Kibler D  Albert M", "D Aha D Kibler and M Albert"),

            Pair.with("Clouse, J., & Utgoff, P.", "Fahlman, S. E.,"),
            Pair.with("Clouse J  Utgoff P", "Fahlman S E"),

            Pair.with("VĂ¸mmĂ¸l Spellmannslag", "V mm l Spellemannslag"),
            Pair.with("FMLSPL", "FMLSPL"),

            Pair.with("The Beta Band", "Raoul And The Big Time"),
            Pair.with("0BTBNT", "RLNT0B")
        );

        for (Pair p : pairs) {
            System.out.println(StringUtils.join(Arrays.asList(p.getValue0(),p.getValue1(),sim.similarity(p.getValue0(), p.getValue1())), "\t"));
        }
    }

}
