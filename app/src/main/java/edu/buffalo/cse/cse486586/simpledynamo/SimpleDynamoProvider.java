package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String REMOTE_PORT0 = "11108";//54
	static final String REMOTE_PORT1 = "11112";//56
	static final String REMOTE_PORT2 = "11116";//58
	static final String REMOTE_PORT3 = "11120";//60
	static final String REMOTE_PORT4 = "11124";//62
	static final String[] ports={REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};
    static String[] sorted_ports={REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};
    static String[] sorted_hashes={REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};

	static final int SERVER_PORT = 10000;
	static String DevId;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static String nodeID;
	static String predecessor="";
	static String successor="";
	static boolean islast=false;
	static boolean isfirst=false;
    static boolean hasResurrected=false;
    static final int timeout=1900;
    static final int add_time=2700;
	static TreeMap<String,String[]> AllNodes = new TreeMap<String, String[]>();
	static HashMap<String,String> hashTOport = new HashMap<String, String>();
	static HashMap<String,String> portTOhash = new HashMap<String, String>();
    static ConcurrentHashMap<String,Integer> KeyVersionIndex = new ConcurrentHashMap<String,Integer>();

	public String findNode(String hashOfKey)
	{
		try
		{

            for (int i = 1; i < sorted_hashes.length; i++)
            {
                if(hashOfKey.compareTo(sorted_hashes[i-1])>0 && hashOfKey.compareTo(sorted_hashes[i])<=0)
                {
                    return sorted_hashes[i];
                }
            }

            if(hashOfKey.compareTo(sorted_hashes[sorted_hashes.length-1])>0 || hashOfKey.compareTo(sorted_hashes[0])<=0)
            {return sorted_hashes[0];}

			return "SthWentWrong!";
		}
		catch (Exception e)
		{
			Log.e(TAG, "Problem Finding CorrectNode!"+e);
		}

		return "SthWentWrong!";
	}
    public String findNodeH(String Key)
    {
        String hashOfKey="";
        try {
            hashOfKey=genHash(Key);
        }
        catch (Exception e)
        {
            Log.e(TAG, "Could not find hashOfKey!! Error: " + e);
        }
        try
        {

            for (int i = 1; i < sorted_hashes.length; i++)
            {
                if(hashOfKey.compareTo(sorted_hashes[i-1])>0 && hashOfKey.compareTo(sorted_hashes[i])<=0)
                {
                    return sorted_hashes[i];
                }
            }

            if(hashOfKey.compareTo(sorted_hashes[sorted_hashes.length-1])>0 || hashOfKey.compareTo(sorted_hashes[0])<=0)
            {return sorted_hashes[0];}

            return "SthWentWrong!";
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem Finding CorrectNode!"+e);
        }

        return "SthWentWrong!";
    }
	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String CorrectNode="@#$%";
        try
        {
            CorrectNode=findNode(genHash(values.getAsString("key")));
            if(CorrectNode.equals("SthWentWrong!"))
                Log.e(TAG, "Error finding correct nodefor insertion1!");
            Log.i(TAG, "CorrectNode=" + CorrectNode);

        }
        catch (Exception e)
        {
            Log.e(TAG,"Error finding correct node for insertion!");
        }
        if(CorrectNode.equals(nodeID)) {
            String filename = values.getAsString("key");
            String string = values.getAsString("value");
            FileOutputStream outputStream;

            try {
                outputStream = getContext().getApplicationContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
                //updateVersionInfoNow
                if(KeyVersionIndex.containsKey(filename)){
                    KeyVersionIndex.put(filename, KeyVersionIndex.get(filename) + 1);
                }
                else{
                    KeyVersionIndex.put(filename,1);
                }
            } catch (Exception e) {
                Log.e(TAG, "File write failed (locally)");
            }

            Log.v("inserted locally", string + "->" + filename);
            int r=insert2successors(CorrectNode, values);
            if(r==1)
            {Log.v(TAG,"inserted into successors as well");}
            else
            {Log.v(TAG,"Not inserted into successors");}
        }
        else
        {
            Log.i(TAG, "Fwding msg to Correct Node: " + CorrectNode);
            int r=MsgToCN_insert(CorrectNode, values);
            if(r==1)
            {Log.v(TAG,"inserted into correct Node");}
            else
            {Log.v(TAG,"Not inserted into correct Node");}
            r=insert2successors(CorrectNode,values);
            if(r==1)
            {Log.v(TAG,"inserted into successors as well");}
            else
            {Log.v(TAG,"Not inserted into successors");}
        }
        return uri;
    }
    public int insert2successors(String CoordinatorNodeID, ContentValues values)
    {
        String suc1=AllNodes.get(CoordinatorNodeID)[1];
        String suc2=AllNodes.get(suc1)[1];
        int s1=MsgToCN_insert(suc1, values);
        int s2=MsgToCN_insert(suc2, values);
        if(s1==1 && s2==1)
        {return 1;}

        return 0;
    }
    public int MsgToCN_insert(String DestinationNodeID, ContentValues values)
    {
        int reply=0;
        try
        {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(hashTOport.get(DestinationNodeID)));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            BufferedReader r= new BufferedReader(new InputStreamReader(socket.getInputStream()));
            w.write("NewData*" + values.getAsString("key")+":"+values.getAsString("value") + "\n");
            w.flush();

            socket.setSoTimeout(timeout);
            String rep=r.readLine();
            if(rep==null)
            {
               // Log.i(TAG,"null reply from CN " + hashTOport.get(DestinationNodeID));
                throw new SocketTimeoutException();
            }
            if(rep.equals("MsgRecvd"))
            {
                Log.i(TAG,"Msg was recvd by CN and inserted properly.");
                reply=1;
            }
            else
                Log.e(TAG, "Msg was NOT recvd by CN. Reply=" + rep);
            socket.close();
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem sending msg to CorrectNode! (MsgToCN_insert) which was "+hashTOport.get(DestinationNodeID)+" "+e);
        }
        finally {

            return reply;
        }

    }

    public int insertHereVersion(String key, String val, String version){
        String filename = key;
        FileOutputStream outputStream;
        /*if(KeyVersionIndex.containsKey(filename) && KeyVersionIndex.get(filename)>=Integer.parseInt(version))
        {
            Log.v("System.out.print", "TooSlow");
            return 1;
        }*/
        try {
            outputStream = getContext().getApplicationContext().openFileOutput(filename, Context.MODE_PRIVATE);
            outputStream.write(val.getBytes());
            outputStream.close();
            //updating version info for the key
            Log.v("System.out.print","InsertHereVersion: "+version);
            KeyVersionIndex.put(filename, Integer.parseInt(version));
        } catch (Exception e) {
            Log.e(TAG, "File write failed (insertHereVersion) " + e);
            return 0;
        }
        Log.v("inserted", val + "->" + filename);
        return 1;
    }

    public int insertHere(String key, String val) {

        String filename = key;
        FileOutputStream outputStream;

        try {
            outputStream = getContext().getApplicationContext().openFileOutput(filename, Context.MODE_PRIVATE);
            outputStream.write(val.getBytes());
            outputStream.close();
            //updating version info for the key
            if(KeyVersionIndex.containsKey(filename)){
                KeyVersionIndex.put(filename, KeyVersionIndex.get(filename) + 1);
            }
            else{
                KeyVersionIndex.put(filename, 1);
            }
        } catch (Exception e) {
            Log.e(TAG, "File write failed (insertHere)");
            return 0;
        }
        Log.v("inserted", val + "->" + filename);
        return 1;
    }
    public int askPredecessors(){
        int reply=0;
        HashMap<String,String>[] hmArr= new HashMap[2];
        hmArr[0]= new HashMap<String, String>();
        hmArr[1]= new HashMap<String, String>();
        HashMap<String,Integer> KeyBestVer = new HashMap<String, Integer>();
        String[] pred_hash=new String[]{portTOhash.get(predecessor), AllNodes.get(portTOhash.get(predecessor))[0]};
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(predecessor));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            socket.setSoTimeout(timeout);
            w.write("GetAllVal" + "\n");
            w.flush();
            ObjectInputStream i = new ObjectInputStream(socket.getInputStream());
            Object Extdata = i.readObject();
            if (Extdata == null) {
                Log.i(TAG, "Null reply. Looks like 1st predecessor at port " + predecessor + " is not active anymore!");
                throw new SocketTimeoutException();
            } else {
                hmArr[0] = (HashMap<String, String>) Extdata;
                w.write("Ack" + "\n");
                w.flush();
                if (!hmArr[0].isEmpty()) {
                    Log.i(TAG, "This node has resurrected! Got data from pre1.");
                    reply++;
                }
            }
            socket.close();
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem sending msg to pre1 at resurrection check! "+e);
        }
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(hashTOport.get(AllNodes.get(portTOhash.get(predecessor))[0])));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            socket.setSoTimeout(timeout);
            w.write("GetAllVal" + "\n");
            w.flush();
            ObjectInputStream i = new ObjectInputStream(socket.getInputStream());
            Object Extdata = i.readObject();
            if (Extdata == null) {
                Log.i(TAG, "Null reply. Looks like 2nd predecessor at port " + hashTOport.get(AllNodes.get(portTOhash.get(predecessor))[0]) + " is not active anymore!");
                throw new SocketTimeoutException();
            } else {
                hmArr[1] = (HashMap<String, String>) Extdata;
                w.write("Ack" + "\n");
                w.flush();
                if (!hmArr[1].isEmpty()) {
                    Log.i(TAG, "This node has resurrected! Got data from pre2.");
                    reply++;
                }
            }
            socket.close();
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem sending msg to pre2 at resurrection check! "+e);
        }

        if(reply!=0) {
            for (int p = 0; p <= 1; p++) {
                if(!hmArr[p].isEmpty()) {
                    for (Map.Entry<String, String> KV : hmArr[p].entrySet()) {
                        String[] val_ver = KV.getValue().split(Pattern.quote("***:"));
                        if (val_ver.length == 2) {
                            if (findNodeH(KV.getKey()).equals(pred_hash[p])) {
                                if (KeyBestVer.containsKey(KV.getKey())){
                                    if((Integer.parseInt(val_ver[1]) > KeyBestVer.get(KV.getKey())))
                                    {
                                     insertHereVersion(KV.getKey(), val_ver[0], val_ver[1]);
                                     KeyBestVer.put(KV.getKey(), Integer.parseInt(val_ver[1]));
                                    }
                                } else{
                                    insertHereVersion(KV.getKey(), val_ver[0], val_ver[1]);
                                    KeyBestVer.put(KV.getKey(), Integer.parseInt(val_ver[1]));
                                }
                            }
                        }
                    }
                }
            }
        }

    return reply;
    }

    public int askSuccessors() {
        int reply=0;
        HashMap<String,String>[] hmArr= new HashMap[2];
        hmArr[0]= new HashMap<String, String>();
        hmArr[1]= new HashMap<String, String>();
        HashMap<String,Integer> KeyBestVer = new HashMap<String, Integer>();
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            socket.setSoTimeout(timeout);
            w.write("GetAllVal" + "\n");
            w.flush();
            ObjectInputStream i = new ObjectInputStream(socket.getInputStream());
            Object Extdata = i.readObject();
            if (Extdata == null) {
                Log.i(TAG, "Null reply. Looks like 1st successor at port " + successor + " is not active anymore!");
                throw new SocketTimeoutException();
            } else {
                hmArr[0] = (HashMap<String, String>) Extdata;
                w.write("Ack" + "\n");
                w.flush();
                if (!hmArr[0].isEmpty()) {
                    Log.i(TAG, "This node has resurrected! Got data from suc1.");
                    reply++;
                }
            }
            socket.close();
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem sending msg to suc1 at resurrection check! "+e);
        }
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(hashTOport.get(AllNodes.get(portTOhash.get(successor))[1])));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            socket.setSoTimeout(timeout);
            w.write("GetAllVal" + "\n");
            w.flush();
            ObjectInputStream i = new ObjectInputStream(socket.getInputStream());
            Object Extdata = i.readObject();
            if (Extdata == null) {
                Log.i(TAG, "Null reply. Looks like 2nd successor at port " + hashTOport.get(AllNodes.get(portTOhash.get(successor))[1]) + " is not active anymore!");
                throw new SocketTimeoutException();
            } else {
                hmArr[1] = (HashMap<String, String>) Extdata;
                w.write("Ack" + "\n");
                w.flush();
                if (!hmArr[1].isEmpty()) {
                    Log.i(TAG, "This node has resurrected! Got data from suc2.");
                    reply++;
                }
            }
            socket.close();
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem sending msg to suc2 at resurrection check! "+e);
        }

        if(reply!=0) {
            for (int p = 0; p <= 1; p++) {
                if(!hmArr[p].isEmpty()) {
                    for (Map.Entry<String, String> KV : hmArr[p].entrySet()) {
                        String[] val_ver = KV.getValue().split(Pattern.quote("***:"));
                        if (val_ver.length == 2) {
                            if (findNodeH(KV.getKey()).equals(nodeID)) {
                                if (KeyBestVer.containsKey(KV.getKey())){
                                    if((Integer.parseInt(val_ver[1]) > KeyBestVer.get(KV.getKey())))
                                    {
                                     insertHereVersion(KV.getKey(), val_ver[0], val_ver[1]);
                                     KeyBestVer.put(KV.getKey(), Integer.parseInt(val_ver[1]));
                                    }
                                } else {
                                    insertHereVersion(KV.getKey(), val_ver[0], val_ver[1]);
                                    KeyBestVer.put(KV.getKey(), Integer.parseInt(val_ver[1]));
                                }
                            }
                        }
                    }
                }
            }
        }
        return reply;
    }

	@Override
	public boolean onCreate() {


		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		final String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		
		DevId=portStr;
		Log.i(TAG, "DevId: "+DevId);

		try
		{
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}
		catch (IOException e)
		{
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}
		nodeID="DUMMY";
        HashMap<String,Integer> tempMap = new HashMap<String,Integer>();

		try
		{

			nodeID = genHash(DevId);
            Integer avdid=5552;

            for (Integer i = 0; i < ports.length; i++)
            {
                avdid=avdid+2;
                hashTOport.put(genHash(avdid.toString()), ports[i]);
                portTOhash.put(ports[i], genHash(avdid.toString()));
                tempMap.put(genHash(avdid.toString()),i);
                
            }

            SortedSet<String> hashes = new TreeSet<String>(tempMap.keySet());
            int j=0;
            for (String id : hashes) {
                Integer index = tempMap.get(id);
                sorted_ports[j]=ports[index];
                sorted_hashes[j]=id;
                j++;
            }
            for (int i=0;i<sorted_hashes.length;i++)
                Log.v("System.out.print", sorted_hashes[i]);
            

            Integer portIndex=Arrays.asList(sorted_ports).indexOf(hashTOport.get(nodeID));
            
            if (portIndex==0)
            {
                predecessor=sorted_ports[sorted_ports.length-1];
                isfirst = true;
                Log.i(TAG, "This is the first node");
            }
            else
            {
                predecessor=sorted_ports[portIndex-1];
            }
            
            if (portIndex==sorted_ports.length-1)
            {
                successor=sorted_ports[0];
                islast = true;
                Log.i(TAG, "This is the last node");
            }
            else
            {
                successor=sorted_ports[portIndex+1];
            }
            
            int i=0;
            for (String id : hashes) {
                if(i==0) {
                    AllNodes.put(id, new String[]{portTOhash.get(sorted_ports[sorted_ports.length-1]), portTOhash.get(sorted_ports[i+1])});
                }
                else if(i==(sorted_ports.length-1)){
                    AllNodes.put(id, new String[]{portTOhash.get(sorted_ports[i-1]), portTOhash.get(sorted_ports[0])});
                }
                else {
                    AllNodes.put(id, new String[]{portTOhash.get(sorted_ports[i - 1]), portTOhash.get(sorted_ports[i+1])});
                }
                i++;
            }

            //SimpleCheck1
            String[] pns=AllNodes.get(nodeID);
            if(predecessor.equals(hashTOport.get(pns[0])) && successor.equals(hashTOport.get(pns[1])))
            {
                Log.v("System.out.print","All good :)");
            }
            else {
                Log.v("System.out.print","Incorrect PnS!!!");}
            //SimpleCheck2
            for (Map.Entry<String, String[]> KV : AllNodes.entrySet()) {
                Log.v("System.out.print", KV.getKey()+":"+ KV.getValue()[0] + " & " +KV.getValue()[1]);
            }
            AllLocalData("Delete");
            //clientTask to check if resurrected
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "JOIN*"+nodeID);
		}
		catch (Exception e)
		{
			Log.e(TAG, "NodeID could not be generated!! Error: " + e);
			return false;
		}
		return true;
	}

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        String[] colnames = {"key", "value"};
        HashMap<String,Integer> KeyBestVer = new HashMap<String, Integer>();
        HashMap<String,String > MCmap = new HashMap<String, String>();
        MatrixCursor m = new MatrixCursor(colnames);
        FileInputStream inputStream;
        ArrayList selection_crit = new ArrayList();
        boolean specialQuery=false;

        if (selection.equals("@") || selection.equals("*"))
        {
            specialQuery=true;
            File dir=getContext().getApplicationContext().getFilesDir();
            File[] files = dir.listFiles();
            for (int i=0; i < files.length; i++)
            {
                selection_crit.add(files[i].getName());
                Log.v("System.out.print","added to selection crit: "+files[i].getName());
            }
        }
        else
            selection_crit.add(selection);

        long k;
        String strval,fname;

        for (Object fname_o : selection_crit)
        {
            fname=fname_o.toString();
            if(specialQuery)
            {

                try {
                    inputStream = getContext().getApplicationContext().openFileInput(fname);
                    k = inputStream.getChannel().size();
                    byte[] b = new byte[(int) k];
                    inputStream.read(b);
                    inputStream.close();
                    strval = new String(b);
                    MCmap.put(fname,strval);
                    KeyBestVer.put(fname,KeyVersionIndex.get(fname));
                    Log.v("query1", fname + " " + strval);
                } catch (Exception e) {
                    Log.e(TAG, "File read failed :X 1; "+e);
                }
            }
            else
            {
                int totalrep=0;
                String CN="";
                try
                {
                    CN=findNode(genHash(fname));
                    Log.i(TAG, "CorrectNode="+CN);
                }
                catch (Exception e)
                {
                    Log.e(TAG,"Error finding correct node for querying!");
                }
                if(CN.equals(nodeID)) {
                    try {
                        inputStream = getContext().getApplicationContext().openFileInput(fname);
                        k = inputStream.getChannel().size();
                        byte[] b = new byte[(int) k];
                        inputStream.read(b);
                        inputStream.close();
                        strval = new String(b);
                        MCmap.put(fname, strval);
                        KeyBestVer.put(fname,KeyVersionIndex.get(fname));
                        totalrep++;
                        Log.v("query2", fname + " " + strval);
                    } catch (Exception e) {
                        Log.e(TAG, "File read failed :X 2; "+e);
                    }
                }
                else
                {
                    String response=MsgToCN_query(CN, fname);
                    String[] val_ver= response.split(Pattern.quote("***:"));
                    if(val_ver.length==2)
                    {
                        MCmap.put(fname, val_ver[0]);
                        KeyBestVer.put(fname,Integer.parseInt(val_ver[1]));
                        totalrep++;
                    }
                }

                //check other 2 replicas

                //suc1
                CN=AllNodes.get(CN)[1];
                String response = MsgToCN_query(CN, fname);
                String[] val_ver= response.split(Pattern.quote("***:"));
                if(val_ver.length==2)
                {
                    if(KeyBestVer.containsKey(fname)){
                        if (Integer.parseInt(val_ver[1])>KeyBestVer.get(fname))
                        {
                            MCmap.put(fname,val_ver[0]);
                            KeyBestVer.put(fname, Integer.parseInt(val_ver[1]));
                            totalrep++;
                        }
                    }
                    else {
                        MCmap.put(fname,val_ver[0]);
                        KeyBestVer.put(fname, Integer.parseInt(val_ver[1]));
                        totalrep++;
                    }
                }

                //suc2
                CN=AllNodes.get(CN)[1];
                response = MsgToCN_query(CN, fname);
                val_ver= response.split(Pattern.quote("***:"));
                if(val_ver.length==2)
                {
                    if(KeyBestVer.containsKey(fname)){
                        if (Integer.parseInt(val_ver[1])>KeyBestVer.get(fname))
                        {
                            MCmap.put(fname,val_ver[0]);
                            KeyBestVer.put(fname, Integer.parseInt(val_ver[1]));
                            totalrep++;
                        }
                    }
                    else {
                        MCmap.put(fname,val_ver[0]);
                        KeyBestVer.put(fname, Integer.parseInt(val_ver[1]));
                        totalrep++;
                    }
                }

                if(totalrep==0)
                {
                    Log.e(TAG, "COULD NOT FIND VALUE FOR KEY# "+ fname +" IN ANY REPLICA! :(");
                }
            }
        }

        if(specialQuery)
        {
            //check other 2 replicas or more
            int Nodes_covered=1,upto_node=3;
            String CN="";
            if (selection.equals("*"))
                upto_node=5;

            for(int i=Nodes_covered+1;i<=upto_node;i++)
            {
                if(i==2)
                {
                    CN=AllNodes.get(nodeID)[1];
                }
                else{
                    CN=AllNodes.get(CN)[1];
                }

                HashMap<String, String> responsehm = MsgToCN_allquery(hashTOport.get(CN));
                if(!responsehm.isEmpty()) {
                    for (Map.Entry<String, String> KV : responsehm.entrySet()) {
                        String[] val_ver = KV.getValue().split(Pattern.quote("***:"));
                        if (val_ver.length == 2) {
                            Log.v(TAG, "val_ver derived from: " + KV.getValue());
                            if (selection.equals("@")) {
                                Log.v("sysout", "K=> "+KV.getKey());
                                if (KeyBestVer.containsKey(KV.getKey())){
                                    Log.v("sysout",KeyBestVer.get(KV.getKey()).toString());
                                    if(Integer.parseInt(val_ver[1]) > KeyBestVer.get(KV.getKey()))
                                    {
                                     MCmap.put(KV.getKey(), val_ver[0]);
                                     KeyBestVer.put(KV.getKey(), Integer.parseInt(val_ver[1]));
                                    }
                                }else if(findNodeH(KV.getKey()).equals(nodeID))
                                {
                                    Log.v(TAG, "GUESS WAS MISSING THIS. Is it EVER used??: "+nodeID+"--"+findNodeH(KV.getKey()));
                                    MCmap.put(KV.getKey(), val_ver[0]);
                                    KeyBestVer.put(KV.getKey(), Integer.parseInt(val_ver[1]));
                                }


                            } else {
                                if (KeyBestVer.containsKey(KV.getKey())){
                                if( Integer.parseInt(val_ver[1]) > KeyBestVer.get(KV.getKey()))
                                    {
                                     MCmap.put(KV.getKey(), val_ver[0]);
                                     KeyBestVer.put(KV.getKey(), Integer.parseInt(val_ver[1]));
                                    }
                                } else {
                                    MCmap.put(KV.getKey(), val_ver[0]);
                                    KeyBestVer.put(KV.getKey(), Integer.parseInt(val_ver[1]));
                                }
                            }
                        }
                    }
                }

            }
        }

        //finally make MC from map
        for (Map.Entry<String, String> kvpair : MCmap.entrySet())
        {
            m.addRow(new Object[]{kvpair.getKey(),kvpair.getValue()});
        }
        return m;
    }

    public String MsgToCN_query(String DestinationNodeID, String key)
    {
        try
        {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(hashTOport.get(DestinationNodeID)));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            BufferedReader r= new BufferedReader(new InputStreamReader(socket.getInputStream()));
            w.write("GetVal*" + key + "\n");
            w.flush();
            socket.setSoTimeout(timeout);
            String val=r.readLine();
            if(val==null)
            {
                Log.i(TAG,"Null reply. Looks like CN is not active!");
                throw new SocketTimeoutException();
            }
            Log.i(TAG, "Value for " + key + " = " + val);
            socket.close();
            return val;
        }
        catch (SocketTimeoutException e){
            Log.e(TAG, "Socket Timeout at MsgToCN_query.");
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem sending msg to CorrectNode! (MsgToCN_query)");
        }

        return "SthWentWrong!";

    }

    public String queryHere(String fname)
    {
        FileInputStream inputStream;
        long k;
        try {
            inputStream = getContext().getApplicationContext().openFileInput(fname);
            k = inputStream.getChannel().size();
            byte[] b = new byte[(int) k];
            inputStream.read(b);
            inputStream.close();
            String strval = new String(b);
            strval=strval+"***:"+KeyVersionIndex.get(fname).toString();
            return strval;

        } catch (Exception e) {
            Log.e(TAG, "File read failed :X 3; "+e);
        }
        return "SthWentWrong!";
    }

    public HashMap<String,String> MsgToCN_allquery(String nextNode)
    {
        HashMap<String,String> KVpairs=new HashMap<String, String>();
        try
        {

            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nextNode));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

            w.write("GetAllVal" + "\n");
            w.flush();
            socket.setSoTimeout(timeout);
            ObjectInputStream i= new ObjectInputStream(socket.getInputStream());
            Object Extdata=i.readObject();
            if(Extdata==null)
            {
                Log.i(TAG,"Null reply. Looks like Node at port "+nextNode+" is not active!");
                throw new SocketTimeoutException();
            }
            w.write("Ack" + "\n");
            w.flush();
            KVpairs= (HashMap<String,String>) Extdata;
            socket.close();
            return KVpairs;
        }catch (SocketTimeoutException e){
            Log.e(TAG, "Socket Timeout at MsgToCN_allquery.");
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem sending msg to CorrectNode for all data!");
        }

        return KVpairs;

    }
    public HashMap<String,String> AllLocalData(String mode)
    {
        HashMap<String,String> KVpairs= new HashMap<String, String>();

        if(mode.equals("Query"))
        {
            Log.i(TAG,"Retrieving all content from this avd for * or @");
            FileInputStream inputStream;
            ArrayList selection_crit = new ArrayList();

            File dir = getContext().getApplicationContext().getFilesDir();
            File[] files = dir.listFiles();
            for (int i = 0; i < files.length; i++)
                selection_crit.add(files[i].getName());

            String fname, strval;
            long k;
            for (Object fname_o : selection_crit)
            {
                fname = fname_o.toString();
                try {
                    inputStream = getContext().getApplicationContext().openFileInput(fname);
                    k = inputStream.getChannel().size();
                    byte[] b = new byte[(int) k];
                    Log.v("System.out.print","C0.5");
                    inputStream.read(b);
                    inputStream.close();
                    strval = new String(b);
                    Log.v("System.out.print","C1 - "+fname);
                    strval=strval+"***:"+KeyVersionIndex.get(fname).toString();
                    Log.v("System.out.print","C2");
                    KVpairs.put(fname, strval);
                    Log.v("query4", fname + " " + strval);
                } catch (Exception e) {
                    Log.e(TAG, "File read failed :X 4; "+e);
                }

            }
        }else if(mode.equals("Delete"))
        {
            Log.i(TAG,"Deleting all content from this avd for * or @");
            ArrayList selection_crit = new ArrayList();

            File dir = getContext().getApplicationContext().getFilesDir();
            File[] files = dir.listFiles();
            for (int i = 0; i < files.length; i++)
                selection_crit.add(files[i].getName());

            String fname;
            int flag=0;
            for (Object fname_o : selection_crit)
            {
                fname = fname_o.toString();
                File file = new File(getContext().getApplicationContext().getFilesDir().getPath() + "/" + fname);
                boolean deleted = file.delete();
                if (deleted)
                {
                    Log.i(TAG, "deleted " + fname + "!");
                    KeyVersionIndex.remove(fname);
                }
                else {
                    Log.e(TAG, "Error in deleting -AllLocalDataDelete!");
                    flag=-1;
                }

            }
            if(flag==0)
                KVpairs.put("Deleted","All");
            else
                KVpairs.put("Deleted","SomeORNone");

        }
        return KVpairs;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        ArrayList selection_crit = new ArrayList();
        boolean specialQuery=false;
        if (selection.equals("@") || selection.equals("*"))
        {
            specialQuery=true;
            File dir=getContext().getApplicationContext().getFilesDir();
            File[] files = dir.listFiles();
            for (int i=0; i < files.length; i++)
                selection_crit.add(files[i].getName());
        }
        else
            selection_crit.add(selection);


        String fname;
        for (Object fname_o : selection_crit)
        {
            fname=fname_o.toString();
            if(specialQuery)
            {

                File file = new File(getContext().getApplicationContext().getFilesDir().getPath() + "/" + fname);
                boolean deleted = file.delete();
                if (deleted)
                {
                    Log.i(TAG, "deleted " + fname + "!");
                    KeyVersionIndex.remove(fname);
                }
                else
                    Log.e(TAG, "Error in deleting -SQ!");
            }
            else
            {
                String CN="";
                try
                {
                    CN=findNode(genHash(fname));
                    Log.i(TAG, "CorrectNode="+CN);
                }
                catch (Exception e)
                {
                    Log.e(TAG,"Error finding correct node for deletion!");
                }
                if(CN.equals(nodeID)) {
                    File file = new File(getContext().getApplicationContext().getFilesDir().getPath() + "/" + fname);
                    boolean deleted = file.delete();
                    if (deleted)
                    {
                        Log.i(TAG, "deleted " + fname + "!");
                        KeyVersionIndex.remove(fname);
                    }
                    else
                        Log.e(TAG, "Error in deleting -locally!");
                }
                else
                {
                    boolean response=MsgToCN_delete(CN, fname);
                    if (response)
                        Log.i(TAG, "Data was deleted at the CN");
                    else
                        Log.e(TAG, "Data was NOT deleted at the CN");

                }

                //check other 2 replicas

                //suc1
                CN=AllNodes.get(CN)[1];
                boolean response=MsgToCN_delete(CN, fname);
                if (response)
                    Log.i(TAG, "Data was deleted at suc1");
                else
                    Log.e(TAG, "Data was NOT deleted at suc1");

                //suc2
                CN=AllNodes.get(CN)[1];
                response=MsgToCN_delete(CN, fname);
                if (response)
                    Log.i(TAG, "Data was deleted at suc2");
                else
                    Log.e(TAG, "Data was NOT deleted at suc2");

            }
        }

        if(specialQuery)
        {
            String nextNode=successor;
            if(selection.equals("@")) {
                int node_processed=1;
                while (node_processed<=2) {
                    boolean response = MsgToCN_alldelete(nextNode);
                    Log.i(TAG, "Data at other avd deleted. t/f?: " + response);
                    nextNode = AllNodes.get(portTOhash.get(nextNode))[1];
                    nextNode = hashTOport.get(nextNode);
                    node_processed++;
                }
            }
            if(selection.equals("*")) {
                while (!nextNode.equals(hashTOport.get(nodeID))) {
                    boolean response = MsgToCN_alldelete(nextNode);
                    Log.i(TAG, "Data at other avd deleted. t/f?: " + response);
                    nextNode = AllNodes.get(portTOhash.get(nextNode))[1];
                    nextNode = hashTOport.get(nextNode);
                }
            }
        }

        return 0;
    }

    public boolean MsgToCN_alldelete(String nextNode)
    {
        try
        {

            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nextNode));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            BufferedReader r= new BufferedReader(new InputStreamReader(socket.getInputStream()));
            w.write("DelAllVal" + "\n");
            w.flush();

            socket.setSoTimeout(timeout);
            String reply=r.readLine();
            /*if(reply==null)
            {
                Log.i(TAG,"Null reply from CN "+ nextNode +" for allDelete");
                throw new SocketTimeoutException();
            }*/
            socket.close();

            if(reply.equals("Yes"))
                return true;
            else
                return false;
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem sending msg to CorrectNode for all data deletion!"+ e);
        }

        return false;

    }

    public boolean MsgToCN_delete(String DestinationNodeID, String key)
    {
        try
        {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(hashTOport.get(DestinationNodeID)));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            BufferedReader r= new BufferedReader(new InputStreamReader(socket.getInputStream()));
            w.write("DelVal*" + key + "\n");
            w.flush();

            socket.setSoTimeout(timeout);
            String val=r.readLine();
            /*if(val==null)
            {
                Log.i(TAG,"Null reply from CN " + hashTOport.get(DestinationNodeID)+" for delete");
                throw new SocketTimeoutException();
            }*/

            Log.i(TAG, "Deletion was done?" + val);
            socket.close();
            if (val.equals("Yes"))
                return true;
            else
                return false;
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem sending msg to CorrectNode! (MsgToCN_delete)" + e);
        }

        return false;
    }
    public String deleteHere(String fname)
    {
        File file = new File(getContext().getApplicationContext().getFilesDir().getPath() + "/" + fname);
        boolean deleted = file.delete();
        if (deleted)
        {
            Log.i(TAG, "deleted " + fname + "!");
            KeyVersionIndex.remove(fname);
            return "Yes";
        }
        else
        {
            Log.e(TAG, "Error in deleting- deleteHere!");
            return "No";
        }
    }
	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


	private class ServerTask extends AsyncTask<ServerSocket, String, Void>
	{

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try
			{
				while (true) {
					Socket s = serverSocket.accept();
					//s.setSoTimeout(timeout+add_time);
					BufferedReader r = new BufferedReader(new InputStreamReader(s.getInputStream()));
					String inp=r.readLine();
					BufferedWriter w = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
					if(inp.substring(0, 7).equals("NewData"))
					{
						String[] KeyVal=inp.substring(8).split(Pattern.quote(":"));
                        Integer retval=insertHere(KeyVal[0],KeyVal[1]);
						if(retval==1) {
                            Log.i(TAG, "Data was recvd and inserted");
                            w.write("MsgRecvd"+"\n");
                            w.flush();
                        }
                        else
                        {
                            w.write("Msg not recvd OR recvd but not inserted!"+"\n");
                            w.flush();
                        }
					}
					else if(inp.substring(0, 6).equals("GetVal"))
					{
						String value= queryHere(inp.substring(7));
						w.write(value+"\n");
						w.flush();
					}
					else if(inp.substring(0, 6).equals("DelVal"))
					{
						String value=deleteHere(inp.substring(7));
						w.write(value+"\n");
						w.flush();
					}
					else if(inp.equals("GetAllVal"))
					{
						Log.i(TAG,"Rcvd req for all local data!");
						HashMap<String,String> KVpairs=AllLocalData("Query");
						ObjectOutputStream o= new ObjectOutputStream(s.getOutputStream());
						o.writeObject(KVpairs);
						o.flush();
                        s.setSoTimeout(timeout);
						String ack=r.readLine();
                        if(ack==null)
                        {
                            Log.i(TAG,"Null acknowledgement from requester. It may have died after making the req");
                            throw new SocketTimeoutException();
                        }
						if(ack.equals("Ack"))
							Log.i(TAG,"Transmission of data successful!");
						else
							Log.e(TAG,"Data not recvd by other AVD!");

					}else if(inp.equals("DelAllVal"))
					{
						Log.i(TAG, "Rcvd req for deleting all local data!");
						HashMap<String,String> KVpairs=AllLocalData("Delete");
						if(KVpairs.get("Deleted").equals("All"))
						{
							w.write("Yes"+"\n");
							w.flush();
							Log.i(TAG, "Deletion of local data successful!");
						}
						else
						{
							w.write("No"+"\n");
							w.flush();
							Log.i(TAG, "Deletion of all local data NOT successful!");
						}


					}

					s.close();
				}

			}
            catch (SocketTimeoutException e) {
                Log.e(TAG, "Requester timed out! "+e);
            }
			catch (IOException e) {
				Log.e(TAG, "socket/DiB issue "+e);
			}

			return null;
		}


	}



	private class ClientTask extends AsyncTask<String, Void, Void> {
		@Override
		protected Void doInBackground(String... msgs) {

            //if resurrected get data from 2 predecessors and 2 successors
            Integer n=0;
            n=askSuccessors();
            if (n!=0)
                Log.v(TAG,"Got data from "+ n.toString() +" successors!");
            n=askPredecessors();
            if (n!=0)
                Log.v(TAG, "Got data from " + n.toString() + " predecessors!");


			return null;
		}
	}

}
