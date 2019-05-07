package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.nfc.Tag;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;


public class SimpleDynamoProvider extends ContentProvider {

    static private final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    //    static String[] REMOTE_PORT = {"11108","11112"};
    static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
    private HashMap<String, String> dbHash = new HashMap<String, String>();
    MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});


    TreeMap<String, String> emuList = new TreeMap<String, String>(new Comparator<String>() {
        public int compare(String lPort, String rPort) {
            try {
                return (genHash(lPort).compareTo(genHash(rPort)));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return 1;
        }
    });

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String key = values.get("key").toString();
        String value = values.get("value").toString();
        String tempEmu;
        String insertMsg = "INSERT#" + key + "_" + value;

        tempEmu = keyPosition(key);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertMsg, tempEmu);
        replicateData(key, value, tempEmu);
        
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        CurrentValues currentValues = new CurrentValues();
        currentValues.setPortNo(getPort());
        currentValues.setEmuId(getEmu());

        try {
            currentValues.setEmuIDHash(genHash(getEmu()));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        emuList.put(CurrentValues.emuId, CurrentValues.emuIDHash);

        String joinMessage = "JOIN#" + CurrentValues.emuId;
        new JoinClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joinMessage);

        return false;

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub

        if (selection.equals("@")) {

            try {
                for (String dbkey : dbHash.keySet()) {

                    String[] myValues = new String[]{dbkey, dbHash.get(dbkey)};
                    cursor.addRow(myValues);
                }

                Log.v("query", selection);
                return cursor;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        else{
            if(dbHash.containsKey(selection))
            {
                String[] myValues = new String[]{selection, dbHash.get(selection)};
                cursor.addRow(myValues);
                return cursor;
            }
            else{

            }

        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String getPort() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.d(TAG, "Get Port = " + myPort);
        return myPort;
    }

    private String getEmu() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myEmu = String.valueOf((Integer.parseInt(portStr)));
        Log.d(TAG, "Get Emu = " + myEmu);
        return myEmu;
    }

    public String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public void printEmuList() {
        Log.e(TAG, "********************** Final List *******************");
        for (String i : emuList.keySet()) {
            Log.e(TAG, " PORTS =  " +i+"  " +emuList.get(i));
        }
    }

    public void printDB() {
//        Log.e(TAG, "*************************************");
        for (String i : dbHash.keySet()) {
            Log.e(TAG, " Data =  " + i);
        }
    }

    public void informSource(String newEmu) {
        String updEmuList = CurrentValues.emuId;

        for (String i : emuList.keySet()) {
            if (i.compareTo(CurrentValues.emuId) != 0) {
                updEmuList += "_" + i;
            }
        }
//        Log.i(TAG, " List = " + updEmuList + " Source = " + newEmu);
        updEmuList = "JOIN-UPDATE#" + updEmuList;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, updEmuList, newEmu);
    }

    public void replicateData(String key, String value, String emuID) {
        String replicateMessage = "REPLICATE#" + key + "_" + value;

        if (emuID.equals(emuList.lastKey())) {
            new ReplicateClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replicateMessage, emuList.firstKey(),emuList.higherKey(emuList.firstKey()));
//            Log.e(TAG,"REPLICATION = "+emuID+" "+emuList.firstKey()+" "+emuList.higherKey(emuList.firstKey()));

        } else if (emuID.equals(emuList.lowerKey(emuList.lastKey()))) {
            new ReplicateClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replicateMessage, emuList.lastKey(),emuList.firstKey());
//            Log.e(TAG,"REPLICATION = "+emuID+" " +emuList.firstKey()+" "+emuList.lastKey()+" "+emuList.firstKey());

        } else {
            new ReplicateClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replicateMessage, emuList.higherKey(emuID),emuList.higherKey(emuList.higherKey(emuID)));
//            Log.e(TAG,"REPLICATION = "+emuID+" " +emuList.firstKey()+" "+emuList.higherKey(emuID)+" "+emuList.higherKey(emuList.higherKey(emuID)));
        }

    }

    public String keyPosition(String key)
    {
        String tempEmu = CurrentValues.emuId;

        while (true) {
            try {
                if (genHash(key).compareTo(emuList.get(tempEmu)) <= 0)
                {
                    if (tempEmu.equals(emuList.firstKey())) {
                        return tempEmu;
                    } else if (genHash(key).compareTo(emuList.get(emuList.lowerKey(tempEmu))) > 0) {
                        return tempEmu;
                    } else {
                        tempEmu = emuList.lowerKey(tempEmu);
                    }
                }
                else
                {
                    if (tempEmu.equals(emuList.lastKey())) {
                        return emuList.firstKey();
                    }
                    else {
                        tempEmu = emuList.higherKey(tempEmu);
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
    }

    //----------------------------------------------------------------------------

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.d(TAG, "Socket Accepted");
            String client_msg;
            try {
                while (true) {
                    Log.e(TAG, "In Server");
                    Socket socket = serverSocket.accept();
                    InputStream inputStream = socket.getInputStream();
                    DataInputStream d = new DataInputStream(inputStream);
                    client_msg = d.readUTF();

                    String message[] = client_msg.split("#");

                    String operation = message[0];


                    Log.e(TAG, "Message received from ClientTask = " + client_msg);

                    if (operation.equals("JOIN")) {
                        String newEmu = message[1];
//                        Log.e(TAG, "Join operation ");
                        try {
                            emuList.put(newEmu, genHash(newEmu));
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }

                        informSource(newEmu);
//                        printEmuList();

                    } else if (operation.equals("JOIN-UPDATE")) {
                        for (String i : message[1].split("_")) {
                            try {
                                emuList.put(i, genHash(i));
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }
//                            printEmuList();
                        }

                    } else if (operation.equals("INSERT")) {
                        String key = message[1].split("_")[0];
                        String value = message[1].split("_")[1];
                        dbHash.put(key, value);
//                        Log.e(TAG, "STORED " + key);
                        try {
                            Log.e(TAG,"----- COMPARING VALUE WITH EmuHash = "+(genHash(key).compareTo(emuList.get(CurrentValues.emuId)))+" IN "+CurrentValues.emuId);
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }


                    } else if (operation.equals("REPLICATE")) {

                        String key = message[1].split("_")[0];
                        String value = message[1].split("_")[1];
//                        Log.e(TAG,"Replicating "+key+" "+value);
                        dbHash.put(key, value);

//                        printDB();
//                        Log.e(TAG,"----- COMPARING VALUE WITH EmuHash = #"+(genHash(key).compareTo(emuList.get(CurrentValues.emuId))));
                    }

                }

            } catch (IOException e) {
                Log.e(TAG, "Server Socket IO exception ");
            }

            return null;
        }

    }

    private class JoinClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Log.e(TAG, "In Client");
                String msg = msgs[0];

                for (String i : REMOTE_PORT) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(i));
                    Log.d(TAG, "Sending message = " + msg);
                    OutputStream outputStream = socket.getOutputStream();
                    DataOutputStream d = new DataOutputStream(outputStream);
                    d.writeUTF(msg);
                    d.flush();
                }

            } catch (Exception e) {
                Log.e(TAG, "" + e);
            }


            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Log.e(TAG, "In Client");
                String msg = msgs[0];
                String emu = msgs[1];

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        (Integer.parseInt(emu) * 2));
                Log.d(TAG, "Sending message = " + msg);
                OutputStream outputStream = socket.getOutputStream();
                DataOutputStream d = new DataOutputStream(outputStream);
                d.writeUTF(msg);
                d.flush();


            } catch (Exception e) {
                Log.e(TAG, "" + e);
            }


            return null;
        }
    }

    private class ReplicateClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Log.e(TAG, "In Client");
                String msg = msgs[0];
                for(int i =1;i<=2;i++) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            (Integer.parseInt(msgs[i]) * 2));
                    Log.d(TAG, "Sending message = " + msg);
                    OutputStream outputStream = socket.getOutputStream();
                    DataOutputStream d = new DataOutputStream(outputStream);
                    d.writeUTF(msg);
                    d.flush();
                }

            } catch (Exception e) {
                Log.e(TAG, "" + e);
            }


            return null;
        }
    }
}