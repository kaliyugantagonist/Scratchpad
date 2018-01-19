import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

public class MetaStoreLinuxClient {

    public static void main(String[] args) throws MalformedURLException, MetaException {

        String clientConfigHiveSiteAbsolutePath = args[0];
        connectRemoteHiveMetastore(clientConfigHiveSiteAbsolutePath);

    }

    private static void connectHiveMetastore(String clientConfigHiveSiteAbsolutePath) throws MetaException, MalformedURLException {

        Configuration configuration = new Configuration();
        /*Start : Commented or un-commented, immaterial ...*/
        configuration.addResource(clientConfigHiveSiteAbsolutePath);
        /*End : Commented or un-commented, immaterial ...*/

        HiveConf hiveConf = new HiveConf(configuration,Configuration.class);
        /*Start : Commented or un-commented, immaterial ...*/
        URL url = new File(clientConfigHiveSiteAbsolutePath).toURI().toURL();
        HiveConf.setHiveSiteLocation(url);
        /*End : Commented or un-commented, immaterial ...*/
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,"thrift://sandbox.hortonworks.com:9083");

        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);

        System.out.println("****************************Metastore client : "+hiveMetaStoreClient);
        System.out.println("****************************Is local metastore ? "+hiveMetaStoreClient.isLocalMetaStore());
        System.out.println(hiveMetaStoreClient.getAllDatabases());

        hiveMetaStoreClient.close();
    }

    private static void connectRemoteHiveMetastore(String clientConfigHiveSiteAbsolutePath) throws MetaException, MalformedURLException {

        //System.setProperty("hadoop.home.dir", "E:\\Omkar\\Development\\Software\\Virtualization");
        //System.setProperty("java.library.path", "E:\\Omkar\\Development\\Software\\Virtualization");

        /*Start : Commented or un-commented, immaterial ...*/
        //System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
        //System.setProperty("java.security.auth.login.config","E:\\Omkar\\Development\\lib\\hdp\\loginconf.ini");
        //System.setProperty("java.security.krb5.conf","E:\\Omkar\\Development\\lib\\hdp\\krb5.conf");
        /*End : Commented or un-commented, immaterial ...*/

        Configuration configuration = new Configuration();
        /*Start : Commented or un-commented, immaterial ...*/
        //configuration.addResource("E:\\Omkar\\Development\\lib\\hdp\\client_config\\HDFS_CLIENT\\core-site.xml");
        //configuration.addResource("E:\\Omkar\\Development\\lib\\hdp\\client_config\\HDFS_CLIENT\\hdfs-site.xml");
        configuration.addResource(clientConfigHiveSiteAbsolutePath);
        //configuration.set("hive.server2.authentication","KERBEROS");
        //configuration.set("hadoop.security.authentication", "Kerberos");
        /*End : Commented or un-commented, immaterial ...*/

        HiveConf hiveConf = new HiveConf(configuration,Configuration.class);
        /*Start : Commented or un-commented, immaterial ...*/
        URL url = new File(clientConfigHiveSiteAbsolutePath).toURI().toURL();
        HiveConf.setHiveSiteLocation(url);
        hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION,"KERBEROS");
        hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL,"hive/_HOST@GLOBAL.SCD.SCANIA.COM");
        hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB,"/etc/security/keytabs/hive.service.keytab");
        hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE,"/etc/security/keytabs/hive.service.keytab");
        /*End : Commented or un-commented, immaterial ...*/

        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,"thrift://l4283t.sss.se.scania.com:9083,thrift://l4284t.sss.se.scania.com:9083");


        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);

        System.out.println("****************************Metastore client : "+hiveMetaStoreClient);
        System.out.println("****************************Is local metastore ? "+hiveMetaStoreClient.isLocalMetaStore());
        System.out.println(hiveMetaStoreClient.getAllDatabases());

        hiveMetaStoreClient.close();
    }
}
