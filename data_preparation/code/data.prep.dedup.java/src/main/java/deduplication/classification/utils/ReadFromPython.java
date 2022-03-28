package deduplication.classification.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class's responsibility is to execute a Python script to calculate the Area under the Precision-Recall Curve.
 * This was merely done as a verification step.
 */
public class ReadFromPython {

    public static Map<String, String> executeFromPython(double[] probabilities, int[] labels) throws InterruptedException, IOException {
        Config config = ConfigFactory.load();
        String scriptDir = config.getString("base_dir") + "code/data.prep.dedup.python/evaluation/";
        String scriptName = "execute_auc_pr.py";
//        System.out.println(scriptPath);
        String parameters = "";
        List<String> labelsList = Arrays.stream(labels).boxed().map(x -> String.valueOf(x)).collect(Collectors.toList());
        parameters += String.join("_", labelsList);
        parameters += " ";
        List<String> probabilitiesList = Arrays.stream(probabilities).boxed().map(x -> String.valueOf(x)).collect(Collectors.toList());
        parameters += String.join("_", probabilitiesList);
//        String param = "0_1 0.5_0.7";
//        String command = "source activate data.prep.dedup.python\n";
//        String command = "";

        String sourceActivate = "source /opt/anaconda/bin/activate data.prep.dedup.python";
//        String pythonEnvironment = "export PYTHONPATH=\"\$\{PYTHONPATH\}:"+scriptDir+"\"";
//        System.out.println(pythonEnvironment);

        String mainCmd = sourceActivate + ";" +  "python3 " + scriptDir + scriptName + " " + parameters;
        String[] command = {
                "/bin/bash",
                "-c",
                mainCmd
//                "cd " + scriptDir,
//                sourceActivate,
//                pythonEnvironment,
//                "python3 " + scriptName + " " + param
        };
//        System.out.println(Arrays.toString(command));

//        command += "python3 /c start python " + scriptPath;
//        Process p = Runtime.getRuntime().exec(command + " " + param );
//        Process p = Runtime.exec(command);
//        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
//        String ret = in.readLine();
//        System.out.println("value is : "+ret);

//        Process p = Runtime.getRuntime().exec(command);
        Process p = Runtime.getRuntime().exec(command);
        p.waitFor();
        BufferedReader outReader = new BufferedReader(new InputStreamReader(p.getInputStream()));

        StringBuffer output = new StringBuffer();
        String line = "";
        while ((line = outReader.readLine())!= null) {
            output.append(line + "\n");
        }

        BufferedReader errReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        StringBuffer error = new StringBuffer();
        while ((line = errReader.readLine())!= null) {
            error.append(line + "\n");
        }

//        System.out.println("Exit code: " + p.exitValue());
//        System.out.println("Error stream: " + error.toString());
//        System.out.println("Output stream:" + output.toString());

        String[] lines = output.toString().split("\n");
        Map<String, String> m = new HashMap<>();
        for (String l : lines) {
            List<String> toksList = Arrays.asList(l.split("_"));
            String metric = toksList.get(0);
            if (metric.equals("aucpr")) {
                metric = "auc_pr";
            } else if (metric.equals("precisions")) {
                metric = "auc_pr_precisions";
            } else if (metric.equals("recalls")) {
                metric = "auc_pr_recalls";
            } else if (metric.equals("thresholds")) {
                metric = "auc_pr_thresholds";
            }

            m.put(metric, String.join("_", toksList.subList(1, toksList.size())));
        }
        return m;
    }

//    public static void main(String[] args) throws IOException, InterruptedException {
//
//    }
}
