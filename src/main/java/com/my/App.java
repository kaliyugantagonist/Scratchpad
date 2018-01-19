package com.my;

import com.sun.org.apache.xpath.internal.SourceTreeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import javax.xml.transform.Result;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws ClassNotFoundException, SQLException, MetaException, MalformedURLException {
        switch (args[0]) {
            case "ZOOKEEPER" : 
            {
                connectHiveUsingZookeeper();
                break;
            }

            case "HIVE_SERVER" :
            {
                connectHiveUsingServerName();
                break;
            }

            case "HIVE_METASTORE" : {

                connectHiveMetastore();

                break;
            }
        }
    }

    private static void connectHiveMetastore() throws MetaException, MalformedURLException {

        //System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
        //System.setProperty("java.security.krb5.conf","C:\\Oracle\\Middleware\\Oracle_Home\\oracle_common\\modules\\datadirect\\kerb5.conf");

        Configuration configuration = new Configuration();
        //configuration.addResource("E:\\Omkar\\Development\\lib\\hdp\\client_config\\HDFS_CLIENT\\core-site.xml");
        //configuration.addResource("E:\\Omkar\\Development\\lib\\hdp\\client_config\\HDFS_CLIENT\\hdfs-site.xml");

        HiveConf hiveConf = new HiveConf(configuration,Configuration.class);
        //URL url = new File("E:\\Omkar\\Development\\lib\\hdp\\client_config\\HIVE_CLIENT\\hive-site.xml").toURI().toURL();
        //hiveConf.setHiveSiteLocation(url);
        //hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,"thrift://l4283t.sss.se.scania.com:9083,thrift://l4284t.sss.se.scania.com:9083");

        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
    }

    private static void connectHiveUsingZookeeperCE() throws ClassNotFoundException, SQLException{
        System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
        System.setProperty("java.security.krb5.conf","C:\\Oracle\\Middleware\\Oracle_Home\\oracle_common\\modules\\datadirect\\kerb5.conf");
        //System.setProperty("sun.security.krb5.debug","true");
        System.out.println("getting connection");
        Connection con = DriverManager.getConnection("jdbc:hive2://l4373t.sss.se.scania.com:2181,l4283t.sss.se.scania.com:2181,l4284t.sss.se.scania.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-hive2");
        System.out.println("Connected ...");

        Statement stmt = con.createStatement();
        //Data
        ResultSet rsData = stmt.executeQuery("use ojoqcu; select * from ojoqcu.employee");
        while(rsData.next()){
            System.out.println("First Name : "+rsData.getString("firstname"));
            System.out.println("Last Name : "+rsData.getString("lastname"));
        }
        stmt.close();


        //Metadata
        stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("show tables");
        while (rs.next()){
            System.out.println("table name : "+rs.getString(1));
        }
        stmt.close();

        DatabaseMetaData databaseMetaData = con.getMetaData();
        //PK
        ResultSet primaryKeysSet = databaseMetaData.getPrimaryKeys("ojoqcu","ojoqcu","employee");
        while (primaryKeysSet.next()){
            System.out.println("PK : "+primaryKeysSet.getString("COLUMN_NAME"));
        }
        primaryKeysSet.close();

        //FK
        ResultSet foreignKeysSet = databaseMetaData.getImportedKeys("ojoqcu","ojoqcu","employee");
        while(foreignKeysSet.next()){
            System.out.println("FK : "+foreignKeysSet.getString("PKCOLUMN_NAME"));
        }
        con.close();
    }

    private static void connectHiveUsingZookeeper() throws ClassNotFoundException, SQLException{
        System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
        System.setProperty("java.security.krb5.conf","C:\\Oracle\\Middleware\\Oracle_Home\\oracle_common\\modules\\datadirect\\kerb5.conf");
        //System.setProperty("sun.security.krb5.debug","true");

        System.out.println("getting connection");
        Connection con = DriverManager.getConnection("jdbc:hive2://l4373t.sss.se.scania.com:2181,l4283t.sss.se.scania.com:2181,l4284t.sss.se.scania.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-hive2");
        System.out.println("Connected ...");

        Statement stmt = con.createStatement();
        //Data
        System.out.println("**********Data**********");
        ResultSet rsData = stmt.executeQuery("select * from ojoqcu.employee");

        while(rsData.next()){
            System.out.println("First Name : "+rsData.getString("firstname"));
            System.out.println("Last Name : "+rsData.getString("lastname"));
        }
        stmt.close();

        //Metadata
        System.out.println("**********MetaData**********");
        stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("show tables");
        while (rs.next()){
            System.out.println("table name : "+rs.getString(1));
        }
        stmt.close();

        DatabaseMetaData databaseMetaData = con.getMetaData();
        //PK
        ResultSet primaryKeysSet = databaseMetaData.getPrimaryKeys("ojoqcu","ojoqcu","employee");
        while (primaryKeysSet.next()){
            System.out.println("PK : "+primaryKeysSet.getString("COLUMN_NAME"));
        }
        primaryKeysSet.close();

        //FK
        ResultSet foreignKeysSet = databaseMetaData.getImportedKeys("ojoqcu","ojoqcu","employee");
        while(foreignKeysSet.next()){
            System.out.println("primary key column name being imported : "+foreignKeysSet.getString("PKCOLUMN_NAME"));
            System.out.println("foreign key column name : "+foreignKeysSet.getString("FKCOLUMN_NAME"));
        }
        foreignKeysSet.close();

       //FK
        ResultSet fkSet = databaseMetaData.getExportedKeys("ojoqcu","ojoqcu","department");
        while(fkSet.next()){
            System.out.println("primary key column name : "+fkSet.getString("PKCOLUMN_NAME"));
            System.out.println("foreign key column name being exported : "+fkSet.getString("FKCOLUMN_NAME"));
        }
        fkSet.close();

        con.close();
    }

    private static void connectHiveUsingServerName() throws ClassNotFoundException, SQLException {
        System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
        System.setProperty("java.security.krb5.conf","C:\\Oracle\\Middleware\\Oracle_Home\\oracle_common\\modules\\datadirect\\kerb5.conf");
        //System.setProperty("sun.security.krb5.debug","true");

        System.out.println("getting connection");
        Connection con = DriverManager.getConnection("jdbc:hive2://l4284t.sss.se.scania.com:10501/;transportMode=http;principal=hive/_HOST@GLOBAL.SCD.SCANIA.COM;httpPath=cliservice");
        System.out.println("Connected");

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("show tables");
        while (rs.next()){
            System.out.println("table name : "+rs.getString(1));
        }
        stmt.close();
        con.close();
    }

    private static void connectHiveUsingServerNameCE() throws ClassNotFoundException, SQLException {
        System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
        System.setProperty("java.security.auth.login.config","E:\\Omkar\\Development\\lib\\jdbc\\KerberosLoginConfig.ini");
        //System.setProperty("java.security.krb5.conf","C:\\kerb5.conf");
        System.setProperty("sun.security.krb5.debug","true");

        System.out.println("getting connection");
        Connection con = DriverManager.getConnection("jdbc:hive2://l4284t.sss.se.scania.com:10501/;transportMode=http;principal=hive/_HOST@GLOBAL.SCD.SCANIA.COM;httpPath=cliservice;AuthMech=1;KrbHostFQDN=l4284t.sss.se.scania.com;KrbServiceName=hive;KrbAuthType=2");
        System.out.println("Connected");

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("show tables");
        while (rs.next()){
            System.out.println("table name : "+rs.getString(1));
        }
        stmt.close();
        con.close();
    }
}
