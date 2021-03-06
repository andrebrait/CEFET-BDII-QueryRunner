package br.cefetmg.bdii;

/**
 * Classe principal do programa.
 * 
 * Os alunos devem substituir os parâmetros indicados com TODO e inserir suas
 * queries.
 * 
 * @author André Brait <andrebrait@gmail.com>
 *
 */
public class Main {

	public static void main(String[] args) {
		// TODO Inserir aqui seu nome de usuário
		String userName = "Seu nome de usuário";

		// TODO Inserir aqui sua senha
		String password = "Sua senha";

		// TODO Inserir aqui o endereço do banco
		String url = "Endereço do banco para conexão";

		// TODO Inserir aqui a porta para conexão (normalmente 1521)
		Integer port = 1521;

		// TODO Inserir aqui o SID (normalmente 'xe')
		String sid = "xe";

		// TODO Mudar para true caso deseje que os resultados da busca sejam impressos
		boolean printMode = false;

		QueryRunner qr = new QueryRunner(userName, password, url, port, sid, printMode);
		try {
			// Tentando estabelecer conexão com o banco
			qr.connect();

			// TODO Exemplo de leitura de queries a partir de arquivo de texto
			for (String query : QueryUtils.parse("sample_queries.sql")) {
				qr.runQuery(query);
			}

			// TODO Rodar suas queries abaixo. O número padrão de repetições é 10
			qr.runQuery("select 1 from dual"); // Exemplo de query
			qr.runQuery("select count(1) from dual"); // Exemplo de query

			// Exibir na saída padrão as estatísticas das consultas.
			qr.printStatistics();
		} finally {
			// Desconectando do banco.
			qr.disconnect();
		}
	}

}
