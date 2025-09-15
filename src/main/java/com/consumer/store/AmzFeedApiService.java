package com.consumer.store;

import com.amazon.SellingPartnerAPIAA.AWSAuthenticationCredentials;
import com.amazon.SellingPartnerAPIAA.AWSSigV4Signer;
import com.consumer.store.model.Client;
import com.consumer.store.model.Item;
import com.consumer.store.model.Setting;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.MediaType;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.springframework.stereotype.Service;
import com.consumer.store.Exceptions.InvalidRefreshTokenException;

import static java.lang.Thread.sleep;
import com.consumer.store.model.Setting;

public class AmzFeedApiService {
    
    private String amz_token,iam_access_keyId,iam_access_secret,iam_region,sp_url;
    private String client_id,client_secret,refresh_token,new_token_url;

    @Autowired
    private Environment env;

    public void setAmz_token(String amz_token) {
        this.amz_token = amz_token;
    }

    public String getAmz_token() {
       
         return this.amz_token;
    }

    public void setIam_access_keyId(String iam_access_keyId) {
        this.iam_access_keyId = iam_access_keyId;
    }

    public void setIam_access_secret(String iam_access_secret) {
        this.iam_access_secret = iam_access_secret;
    }

    public void setSp_url(String sp_url) {
        this.sp_url = sp_url;
    }

    public void setNew_token_url(String new_token_url) {
        this.new_token_url = new_token_url;
    }

    public void setIam_region(String iam_region) {
        this.iam_region = iam_region;
    }

    public void setClient_id(String client_id) {
        this.client_id = client_id;
    }

    public void setClient_secret(String client_secret) {
        this.client_secret = client_secret;
    }

    public void setRefresh_token(String refresh_token) {
        this.refresh_token = refresh_token;
    }

    // public void initialize(Client client,Session session){
    //     Configuration configuration = new Configuration();
    //     // File f = new File("../../../../../resources/hibernate.cfg.xml");
    //     configuration.configure();
    //     configuration.setProperties(client_db);
    //     SessionFactory sessionFactory = configuration.buildSessionFactory();
    //     Session session = sessionFactory.openSession();
    //     Setting token = session.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.token_key"));
    //     Setting refresh_token = session.bySimpleNaturalId(Setting.class).load(env.getProperty("spring.integration.amazon.refresh_token_key"));
    //     //Setting token_expire_in = session.bySimpleNaturalId(Setting.class).load(env.getProperty("amazon_sp_token_expired"));
    //     this.setAmz_token(token.getValue());
    //     this.setRefresh_token(refresh_token.getValue());
    //     this.setClient_id(env.getProperty("spring.integration.amazon.client_id"));
    //     this.setClient_secret(env.getProperty("spring.integration.amazon.client_secret"));
    //     this.setIam_access_keyId(env.getProperty("spring.integration.amazon.iam_access_keyID"));
    //     this.setIam_access_secret(env.getProperty("spring.integration.amazon.iam_access_secretKey"));
    //     this.setNew_token_url(env.getProperty("spring.integration.amazon.token_url"));
    //     this.setIam_region(env.getProperty("spring.integration.amazon.iam_region"));
    //     this.setSp_url(env.getProperty("feed_api_url"));
    //     session.close();
    //     // sessionFactory.close();


    // }

    public JSONObject createFeedDocument() throws  Exception, InvalidRefreshTokenException{
        com.squareup.okhttp.OkHttpClient okHttpClient=new com.squareup.okhttp.OkHttpClient();

        for (int i=0;i<10;i++) {
            try {
                String json="{\"contentType\":\"text/xml; charset=UTF-8\"}";
                com.squareup.okhttp.RequestBody requestBody=RequestBody
                        .create(MediaType.parse("application/json; charset=utf-8"),json);
                com.squareup.okhttp.Request request_get_feed_document = new Request.Builder()
                        .url(this.sp_url+"/feeds/2021-06-30/documents") // Date time in UTC
                        .post(requestBody)
                        .addHeader("x-amz-access-token", this.amz_token)//db value
                        .addHeader("Accept","application/json")
                        .build();
                AWSAuthenticationCredentials awsAuthenticationCredentials = AWSAuthenticationCredentials.builder()
                        .accessKeyId(this.iam_access_keyId)//env value
                        .secretKey(this.iam_access_secret)// env value
                        .region(this.iam_region)//env value
                        .build();
                com.squareup.okhttp.Request request_get_feed_document_signed = new AWSSigV4Signer(awsAuthenticationCredentials)
                        .sign(request_get_feed_document);
                Response get_feed_document_response = okHttpClient.newCall(request_get_feed_document_signed).execute();

                if (get_feed_document_response.code() >= 200 && get_feed_document_response.code()<300) {
                     System.out.println("feed document upload url and feed document id generated");
                     return (new JSONObject(get_feed_document_response.body().string()));
                } else if(get_feed_document_response.code()==404){
                    try {
                        sleep(1000 * 1 * i);//env value
                        continue;
                    } catch (InterruptedException e) {
                        throw new Exception("sleep function failed");
                    }
                }else if(get_feed_document_response.code()==403){
                    this.setAmz_token(this.createNewToken());
                    i--;
                    continue;
                }
                else{
                    throw new Exception("create feed document error:"+get_feed_document_response.body().string());
                }
            }catch( InvalidRefreshTokenException irex ){
                throw irex;
            }
            catch (Exception e){
                e.printStackTrace();
                throw new Exception("request error:"+ e.getMessage());
            }
        }
        throw new Exception("create feed document error: api doesn't response");

    }



    public void xmlUpload(String url,String filepath) throws Exception{
        com.squareup.okhttp.OkHttpClient client = new com.squareup.okhttp.OkHttpClient();
        String content="",line;
        File file=new File(filepath);
        FileReader fileReader=new FileReader(file);
        BufferedReader bufferedReader=new BufferedReader(fileReader);
        while ((line=bufferedReader.readLine())!=null){
            content=content+line;
        }
        String contentType = String.format("text/xml; charset=%s", StandardCharsets.UTF_8);
        for (int i=0;i<10;i++) {
            try {
                Request request = new Request.Builder()
                        .url(url)
                        .put(RequestBody.create(MediaType.parse(contentType), content.getBytes(StandardCharsets.UTF_8)))
                        .build();

                Response response = client.newCall(request).execute();
                if (response.code()>=200 && response.code()<300) {
                    System.out.println("xml upload successful");
                    return;
                } else if(response.code()==404){
                    sleep(1000 * 1 * i);
                    continue;
                }else {
                    throw new Exception("upload error:"+response.body().string());
                }
            } catch (IOException e) {
                //e.printStackTrace();
                throw new Exception("xml upload request error");
            }
        }
        throw new Exception("xml upload error: api does not response");
    }

    public JSONObject createFeed(String mid,String feed_document_id) throws Exception, InvalidRefreshTokenException{
        com.squareup.okhttp.OkHttpClient okHttpClient=new com.squareup.okhttp.OkHttpClient();
        for (int i=0; i < 10; i++){
            try {
                String json_request_string = "{\"feedType\":\"" +
                        "POST_INVENTORY_AVAILABILITY_DATA\",\"" +
                        "marketplaceIds\": [\"" + mid + "\"]," +
                        "\"inputFeedDocumentId\":\"" + feed_document_id + "\"}";
                //System.out.println(json_request_string);
                com.squareup.okhttp.RequestBody create_feed_request_body
                        = RequestBody.create(MediaType.parse("application/json; charset=utf-8")
                        , json_request_string);
                com.squareup.okhttp.Request create_feed_request = new Request.Builder()
                        .url(this.sp_url+"/feeds/2021-06-30/feeds")//env value
                        .post(create_feed_request_body)
                        .addHeader("x-amz-access-token", this.amz_token)//db value
                        .addHeader("Accept", "application/json")
                        .build();
                AWSAuthenticationCredentials awsAuthenticationCredentials = AWSAuthenticationCredentials.builder()
                        .accessKeyId(this.iam_access_keyId)//env value
                        .secretKey(this.iam_access_secret)// env value
                        .region(this.iam_region)//env value
                        .build();
                com.squareup.okhttp.Request create_feed_request_signed=
                        new AWSSigV4Signer(awsAuthenticationCredentials)
                                .sign(create_feed_request);

                com.squareup.okhttp.Response create_feed_response = okHttpClient
                        .newCall(create_feed_request_signed)
                        .execute();
                if(create_feed_response.code()>=200 && create_feed_response.code()<300){
                    return (new JSONObject(create_feed_response.body().string()));
                } else if (create_feed_response.code()==404 ){
                    sleep(1000 * 1 * i);
                    continue;
                }else if(create_feed_response.code()==403){
                    this.setAmz_token(this.createNewToken());
                    i--;
                    continue;
                } else{
                    throw new Exception("create feed request error:"+create_feed_response.body().string());
                }

            }catch( InvalidRefreshTokenException irex ){
                throw irex;
            }
            catch (Exception e){
                 //e.printStackTrace();
                 throw new Exception("create feed request error");
            }
        }
        throw new Exception("create feed error: api doesn't response");
    }
    /*
    public JSONObject  checkProcessStatus(String feed_id) throws Exception{
        OkHttpClient okHttpClient=new OkHttpClient();

        for (int i=0; i< 20 ; i++){
            try {
                com.squareup.okhttp.Request check_status_request = new Request.Builder()
                        .url(this.sp_url+"/feeds/2021-06-30/feeds/"+feed_id)//env value
                        .get()
                        .addHeader("x-amz-access-token", this.amz_token)//db value
                        .addHeader("Accept", "application/json")
                        .build();
                AWSAuthenticationCredentials awsAuthenticationCredentials = AWSAuthenticationCredentials.builder()
                        .accessKeyId(this.iam_access_keyId)//env value
                        .secretKey(this.iam_access_secret)// env value
                        .region(this.iam_region)//env value
                        .build();
                com.squareup.okhttp.Request check_status_request_signed=
                        new AWSSigV4Signer(awsAuthenticationCredentials)
                                .sign(check_status_request);

                com.squareup.okhttp.Response check_status_response = okHttpClient
                        .newCall(check_status_request_signed)
                        .execute();
                if(check_status_response.code()>=200 && check_status_response.code()<300){
                    JSONObject res = new JSONObject(check_status_response.body().string());
                    if(!res.getString("processingStatus").equals("DONE")){
                        sleep(5000 * 1 *i);
                        System.out.println("feed document process:"+res.getString("processingStatus"));
                        continue;
                    }else{
                        System.out.println("feed document process:"+res.getString("processingStatus"));
                        return (res);
                    }
                } else if (check_status_response.code()==404 ){
                    sleep(1000 * 1 * i);
                    continue;
                }else if(check_status_response.code()==403){
                    this.setAmz_token(this.createNewToken());
                     i--;
                     continue;
                } else{
                    throw new Exception("feed status check request error:"+check_status_response.body().string());
                }

            }catch (Exception e){
                e.printStackTrace();
                throw new Exception("feed status check request error");
            }
        }
       throw new Exception("feed status check error: api doesn't response");

    }
    */
    /*
    // public JSONObject  getXMLDownloadUrl(String feed_document_id) throws Exception{
    //     OkHttpClient okHttpClient=new OkHttpClient();

    //     for (int i=0; i< 10 ; i++){
    //         try {
    //             com.squareup.okhttp.Request request = new Request.Builder()
    //                     .url(this.sp_url+"/feeds/2021-06-30/documents/"+feed_document_id)//env value
    //                     .get()
    //                     .addHeader("x-amz-access-token", this.amz_token)//db value
    //                     .addHeader("Accept", "application/json")
    //                     .build();
    //             AWSAuthenticationCredentials awsAuthenticationCredentials = AWSAuthenticationCredentials.builder()
    //                     .accessKeyId(this.iam_access_keyId)//env value
    //                     .secretKey(this.iam_access_secret)// env value
    //                     .region(this.iam_region)//env value
    //                     .build();
    //             com.squareup.okhttp.Request request_signed=
    //                     new AWSSigV4Signer(awsAuthenticationCredentials)
    //                             .sign(request);

    //             com.squareup.okhttp.Response response = okHttpClient
    //                     .newCall(request_signed)
    //                     .execute();
    //             if(response.code()>=200 && response.code()<300){
    //                 JSONObject res = new JSONObject(response.body().string());
    //                 System.out.println("get xml download url: done");
    //                 return (res);

    //             } else if (response.code()==404 ){
    //                 sleep(1000 * 1 * i);
    //                 continue;
    //             }else if(response.code()==403){
    //                 this.setAmz_token(this.createNewToken());
    //                 i--;
    //                 continue;
    //             } else{
    //                 throw new Exception("get xml download url request error:"+response.body().string());
    //             }

    //         }catch (Exception e){
    //             e.printStackTrace();
    //             throw new Exception("get xml download url request error");
    //         }
    //     }
    //     throw new Exception("get xml download url error: api doesn't response");

    // }
    // public void xmlDownload(String url) throws IOException, IllegalArgumentException {
    //     OkHttpClient httpclient = new OkHttpClient();
    //     Request request = new Request.Builder()
    //             .url(url)
    //             .get()
    //             .build();

    //     Response response = httpclient.newCall(request).execute();
    //     if (!response.isSuccessful()) {
    //         System.out.println(
    //                 String.format("Call to download content was unsuccessful with response code: %d and message: %s",
    //                         response.code(), response.message()));
    //         return;
    //     }

    //     try (ResponseBody responseBody = response.body()) {
    //         MediaType mediaType = MediaType.parse(response.header("Content-Type"));
    //         Charset charset = mediaType.charset();
    //         if (charset == null) {
    //             throw new IllegalArgumentException(String.format(
    //                     "Could not parse character set from '%s'", mediaType.toString()));
    //         }
    //         if(mediaType.type().equals("text") && mediaType.subtype().equals("xml")
    //          && mediaType.charset().equals("charset=UTF-8")){
    //             throw  new IllegalArgumentException("response is not xml or charset=UTF-8");
    //         }else{
    //            try{
    //                String fileName="inventory_progress_report.xml";
    //                BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    //                writer.write(responseBody.string());
    //                writer.close();
    //                System.out.println("xml download: done");
    //            }catch (Exception e){
    //                throw new IOException("file write error");
    //            }
    //         }
    //     }
    // }
    */
    public String  createNewToken() throws Exception, InvalidRefreshTokenException{
        com.squareup.okhttp.OkHttpClient okHttpClient=new com.squareup.okhttp.OkHttpClient();
        for (int i=0; i< 10 ; i++){
            try {
                MediaType CONTENT_TYPE = MediaType.parse("application/x-www-form-urlencoded");
                com.squareup.okhttp.RequestBody formBody = new com.squareup.okhttp.FormEncodingBuilder()
                        .add("grant_type", "refresh_token")
                        .add("refresh_token",this.refresh_token)
                        .add("client_id",this.client_id)
                        .add("client_secret",this.client_secret)
                        .build();
                com.squareup.okhttp.Request create_token_request = new Request.Builder()
                        .url(this.new_token_url)//env value
                        .post(formBody)
                        .addHeader("x-amz-access-token", this.amz_token)//db value
                        .addHeader("Accept", "application/json")
                        .build();
                AWSAuthenticationCredentials awsAuthenticationCredentials = AWSAuthenticationCredentials.builder()
                        .accessKeyId(this.iam_access_keyId)//env value
                        .secretKey(this.iam_access_secret)// env value
                        .region(this.iam_region)//env value
                        .build();
                com.squareup.okhttp.Request create_token_request_signed=
                        new AWSSigV4Signer(awsAuthenticationCredentials)
                                .sign(create_token_request);

                com.squareup.okhttp.Response create_feed_response = okHttpClient
                        .newCall(create_token_request_signed)
                        .execute();
                if(create_feed_response.code()>=200 && create_feed_response.code()<300){
                    JSONObject res = new JSONObject(create_feed_response.body().string());
                    System.out.println("token refresh: done");
                    return (res.getString("access_token"));
                } else if (create_feed_response.code()==404 ){
                    sleep(1000 * 1 * i);
                    continue;
                }else{
                     throw new InvalidRefreshTokenException("May need re auth the service");
                    //throw new Exception("create token request error:"+create_feed_response.body().string());
                }

            }catch (Exception e){
                //e.printStackTrace();
                throw e;
            }
        }
        throw new Exception("create token error: api doesn't response");

    }

    public void createXMLFile(String mid,List<Item> items,String filepath) throws Exception{
        try {
            
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document document = documentBuilder.newDocument();
            Element root = document.createElement("AmazonEnvelope");
            Attr attr1 = document.createAttribute("xmlns:xsi");
            attr1.setValue("http://www.w3.org/2001/XMLSchema-instance");
            root.setAttributeNode(attr1);
            Attr attr2 = document.createAttribute("xsi:noNamespaceSchemaLocation");
            attr2.setValue("amzn-envelope.xsd");
            root.setAttributeNode(attr2);
            Element header= document.createElement("Header");
            Element merchantIdentifier=document.createElement("MerchantIdentifier");
            Element documentVersion= document.createElement("DocumentVersion");
            documentVersion.appendChild(document.createTextNode("1.01"));
            merchantIdentifier.appendChild(document.createTextNode(mid));
            header.appendChild(documentVersion);
            header.appendChild(merchantIdentifier);
            root.appendChild(header);
            Element messageType=document.createElement("MessageType");
            messageType.appendChild(document.createTextNode("Inventory"));
            root.appendChild(messageType);
            for (int i=0;i<items.size();i++){
                Element message=document.createElement("Message");
                Element messageId=document.createElement("MessageID");
                Element operationType=document.createElement("OperationType");
                operationType.appendChild(document.createTextNode("PartialUpdate"));
                messageId.appendChild(document.createTextNode(String.valueOf(i+1)));
                Element inventory=document.createElement("Inventory");
                Element sku=document.createElement("SKU");
                if(items.get(i).getAmazon_sku_alias()!=null && items.get(i).getAmazon_sku_alias().isEmpty()!=true){
                    sku.appendChild(document.createTextNode(items.get(i).getAmazon_sku_alias()));
                }else{
                    sku.appendChild(document.createTextNode(items.get(i).getSku()));
                }
                Element quantity=document.createElement("Quantity");
                quantity.appendChild(document.createTextNode(String.valueOf(items.get(i).getQuantity())));
                inventory.appendChild(sku);
                inventory.appendChild(quantity);
                message.appendChild(messageId);
                message.appendChild(operationType);
                message.appendChild(inventory);
                root.appendChild(message);
                sleep(100 * 1);
            }
            document.appendChild(root);
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource domSource = new DOMSource(document);
            StreamResult streamResult = new StreamResult(new File(filepath));
            transformer.transform(domSource, streamResult);
            System.out.println("feed api xml create done");
        }catch (ParserConfigurationException pce){
            pce.printStackTrace();
            throw  new Exception("parser config exception");
        }catch (TransformerException tfe){
            tfe.printStackTrace();
            throw new Exception("xml transform exception");
        }
    }
}
