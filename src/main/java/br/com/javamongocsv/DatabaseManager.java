/*
 * Copyright (c) 2018 Hash Technology. Todos os direitos reservados.
 * Este software é confidencial e um produto proprietário da Hash Technology.
 * Qualquer uso não autorizado, reprodução ou transferência deste software é terminantemente proibida. 
 */
package br.com.javamongocsv; 

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.Serializable;
import org.bson.Document;

/**
 * Classe reponsavel por gerenciar o acesso ao banco de dados mongodb.
 *  
 */
 
public class DatabaseManager implements Serializable {

	private static final long serialVersionUID = 1427551462013092880L;
	private static MongoClient mongoClient;
    private static MongoDatabase database;

     
    public void startup() {
        //cria uma conexao com o mongodb
        mongoClient = new MongoClient();
        //acessa o database analytics
        database = mongoClient.getDatabase("everson");
    }
    
    /**
     * Retorna uma referencia da colecao solicitada.
     * @param collectionName nome da colecao na base de dados.
     * @return retorna a referencia para colecao solicitada
     */
    public MongoCollection<Document> getCollection(String collectionName) {
        return database.getCollection(collectionName);
    }
    
   
    public void shutdown() {
        //fecha a conexao com o mongo
        mongoClient.close();
    }
}
