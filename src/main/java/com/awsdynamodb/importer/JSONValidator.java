package com.awsdynamodb.importer;

import com.fasterxml.jackson.core.JsonParser;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by ahadcse on 26/05/16.
 */
public class JSONValidator {

    public static void main(String args[]) throws IOException {
        File file = new File("data.csv");
        BufferedReader br = new BufferedReader(new FileReader(file));

        String line = null;
        int counter = 0;
        while ((line = br.readLine()) != null) {
            //System.out.println(line);
            counter++;
            if(!isJSONValid(line)) {
                System.out.print("Invalid line number: " + counter);
                throw new JSONException("JSON String not valid");
            }
        }
        System.out.println("Valid JSON File");
        br.close();
    }

    private static boolean isJSONValid(String test) {
        try {
            new JSONObject(test);
        } catch (JSONException ex) {
            try {
                new JSONArray(test);
            } catch (JSONException ex1) {
                return false;
            }
        }
        return true;
    }

}
