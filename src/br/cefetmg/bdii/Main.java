package br.cefetmg.bdii;

public class Main {

	public static void main(String[] args) {
		// TODO Inserir aqui seus dados de login
		String userName = "Seu nome de usuário";
		String password = "Sua senha";

		// TODO Inserir aqui o endereço do banco e porta para conexão (normalmente 1521)
		String url = "Endereço do banco para conexão";
		Integer port = 1521;

		// TODO Inserir aqui o SID (normalmente será 'xe')
		String sid = "xe";
		
		// TODO Mudar para true caso deseje que os resultados da busca sejam impressos
		boolean printMode = false;
		
		QueryRunner qr = new QueryRunner(userName, password, url, port, sid, printMode);
		qr.connect();
		
		// TODO Rodar suas queries abaixo, com o número de repetições desejado
		qr.runQuery("select 1 from dual", 10);
		qr.runQuery("select count(1) from dual", 10);
		
		qr.printStatistics();
		
		// Deve ser executado ao final do programa
		qr.disconnect();
	}

}
