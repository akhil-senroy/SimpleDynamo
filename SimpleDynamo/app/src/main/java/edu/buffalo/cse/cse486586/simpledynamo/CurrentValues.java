package edu.buffalo.cse.cse486586.simpledynamo;
import android.app.Application;

public class CurrentValues {
    static String emuId="";
    static String portNo="";
    static String emuIDHash="";
//    static String firstSuccessor="";
//    static String secondSuccessor="";

    public void setPortNo(String portNo) {
        CurrentValues.portNo = portNo;
    }

    public void setEmuId(String emuId) {
        CurrentValues.emuId = emuId;
    }

    public void setEmuIDHash(String emuIDHash) {
        CurrentValues.emuIDHash = emuIDHash;
    }
//
//    public void setFirstSuccessor(String firstSuccessor) {
//        CurrentValues.firstSuccessor = firstSuccessor;
//    }
//
//    public void setSecondSuccessor(String secondSuccessor) {
//        CurrentValues.secondSuccessor = secondSuccessor;
//    }

}
