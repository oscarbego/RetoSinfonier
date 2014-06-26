package com.sinfonier.spouts;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;


public class SpOutBego extends BaseSinfonierSpout {

       private File file;
       private Scanner sc;
      
    public SpOutBego(String spoutName, String xmlPath) {
        
        super(xmlPath, spoutName);
      
    }

    public void open() {
      
        // TO-DO: Init values. Code here runs once.
        // In Spouts this function is very important. Must get an object than can
        // iterate to use it in usernextTuple()

       file = new File((String)this.getParam("file"));

       try 
        {
            sc = new Scanner(file);
        } 
       catch (FileNotFoundException e) 
        {
            e.printStackTrace();
        }
    }
   

    public void nextTuple(){
      
        // TO-DO: Write code here. This code reads an input tuple by each execution
        // You can use the same functions as in the Bolts to process it.
        // Tipically is to use this.addField to build the Tuple to emit.

       if (sc.hasNextLine()) 
       {
        String row = sc.nextLine();
        System.out.println("SC ROW: "+row);
        String[] splitted = row.split(",");

        this.addField(splitted[0], splitted[1]);
           
        this.emit();
       }
    }
}