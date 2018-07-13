
import com.mongodb.*;
import redis.clients.jedis.Jedis;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Created by Hector on 16/05/2017.
 */
public class Main {

    static Mongo m;

    static {
        try {
            m = new Mongo("localhost", 27017);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    static Jedis jedis = new Jedis("localhost",6379);
    static DB db = m.getDB("local");
    static DBCollection collection = db.getCollection("clientes");

    static int hits = 0;
    static int miss = 0;


    public static void main(String[] a) throws UnknownHostException {
        //List<Cliente> clientes = generarClientes();
        //insertarDatos(clientes);
        long tiempoSinCache = leerDatosSinCache();
        long tiempoTotalCache = leerDatosConCache();
        //eliminarLlaves();

        System.out.println("El tiempo total sin cache es de: "+tiempoSinCache+" milisegundos");
        System.out.println("El tiempo total con cache es de: "+tiempoTotalCache+" milisegundos");
        System.out.println("hits: "+hits);
        System.out.println("miss: "+miss);
    }

    private static void eliminarLlaves(){
        for(int i=0; i<1000;i++){

            if(jedis.exists("llave"+(i+1))){

                jedis.del("llave"+(i+1));
                System.out.println("Eliminando llave: "+(i+1));
            }
        }
    }

    private static long leerDatosConCache() throws UnknownHostException {
        Mongo m = new Mongo("localhost", 27017);

        Random r = new Random();
        Date date = new Date();
        long tiempoTotal;
        long tiempoInicial = date.getTime();




        for(int i=0; i<100000;i++){

            int llave = (int) (r.nextDouble() * 1000 + 1);

            if(!jedis.exists("llave"+llave)){

                DBObject query = new BasicDBObject("idCliente", llave);
                DBCursor cursor = collection.find(query);

                System.out.println("MONGO "+cursor.next());
                jedis.set("llave"+llave , cursor.toString());

                miss++;
            }else{
                jedis.get("llave"+llave);
                System.out.println("JEDIS "+jedis.get("llave"+llave));
                hits++;
            }
        }



        Date date2 = new Date();
        long tiempoFinal = date2.getTime();

        tiempoTotal = tiempoFinal - tiempoInicial;

        return tiempoTotal;
    }

    public static long leerDatosSinCache() throws UnknownHostException {

        Mongo m = new Mongo("localhost", 27017);

        Date date = new Date();
        long tiempoTotal;
        long tiempoInicial = date.getTime();



        Random r = new Random();
        for(int i = 0;i < 100000; i++){
            int random =(int) (r.nextDouble() * 1000 + 1);
            DBObject query = new BasicDBObject("idCliente", random);
            DBCursor cursor = collection.find(query);

            //System.out.println(random+" :"+cursor.next());
            System.out.println("Leyendo "+(i+1)+" registros");
        }
        Date date2 = new Date();
        long tiempoFinal = date2.getTime();

        tiempoTotal = tiempoFinal - tiempoInicial;


        return tiempoTotal;
    }

    public static void insertarDatos(List<Cliente> clientes) throws UnknownHostException, MongoException{
        try{
            Mongo m = new Mongo("localhost", 27017);
            DB db = m.getDB("local");
            DBCollection collection = db.getCollection("clientes");
            for (Cliente cliente : clientes){
                BasicDBObject objeto = new BasicDBObject();

                objeto.append("idCliente",cliente.getIdCliente());
                objeto.append("nombre",cliente.getNombre());
                objeto.append("grupo",cliente.getGrupo());
                objeto.append("direccion",new BasicDBObject()
                        .append("calle",cliente.getDireccion().getCalle())
                        .append("codigoPostal",cliente.getDireccion().getCodigoPostal())
                        .append("estado",cliente.getDireccion().getEstado())
                        .append("numero",cliente.getDireccion().getNumero())
                );
                collection.insert(objeto);
            }

        }catch (MongoException e){
            System.out.println("Error" +e.getMessage());
        }


    }

    public static List<Cliente> generarClientes(){

        List<Cliente> clientes = new ArrayList<Cliente>();



        for(int i = 0;i < 1000; i++){
            Random random = new Random();
            Random random2 = new Random();
            Cliente cliente = new Cliente();

            cliente.setNombre("Hector"+(i+1));
            cliente.setGrupo("Grupo"+random.nextInt(1000));
            cliente.setIdCliente((i+1));

            Direccion direccion = new Direccion();
            direccion.setCalle("Calle"+random.nextInt(1000));
            direccion.setCodigoPostal(""+random.nextInt(1000));
            direccion.setEstado("Sinaloa"+random.nextInt(1000));
            direccion.setNumero(""+random.nextInt(1000));

            cliente.setDireccion(direccion);

            clientes.add(cliente);
        }

        return clientes;
    }
}
