package br.cefetmg.bdii;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Classe QueryRunner.
 * 
 * Esta classe contém métodos auxiliares para executar abrir conexões, executar
 * queries, imprimir resultados, gerar estatísticas e fechar conexões.
 * 
 * @author André Brait <andrebrait@gmail.com>
 *
 */
public class QueryRunner {

	private static final int STANDARD_PRECISION = 3;
	private static final int EXTENDED_PRECISION = 5;

	private static final BigDecimal ZERO = BigDecimal.ZERO.setScale(STANDARD_PRECISION);
	private static final BigDecimal TWO = new BigDecimal(2).setScale(STANDARD_PRECISION);
	private static final BigDecimal THOUSAND = new BigDecimal(1000).setScale(STANDARD_PRECISION);

	public static final int DEFAULT_FETCH_SIZE = 10000;
	public static final int DEFAULT_NUM_EXECUTIONS = 10;

	/**
	 * Classe QueryRunnerResult
	 * 
	 * Representa os resultados de uma execução de busca em banco de dados.
	 * 
	 * @author André Brait <andrebrait@gmail.com>
	 *
	 */
	public static class QueryRunnerResult {
		private final ResultSet rs;
		private ResultSetMetaData rsmd;
		private final List<String[]> results;
		private long executionTime;
		private Integer rowCount;

		private QueryRunnerResult(ResultSet resultSet) {
			this.rs = resultSet;
			this.results = new ArrayList<String[]>();
			this.executionTime = 0L;
		}

		private long getExecutionTime() {
			return this.executionTime;
		}

		private void setExecutionTime(Date inicio, Date fim) {
			this.executionTime = Math.abs(fim.getTime() - inicio.getTime());
		}

		public int getRowCount() {
			if (this.rowCount != null) {
				return this.rowCount;
			}
			return this.results.size();
		}

		private void fetchResults() throws SQLException {
			this.results.clear();
			this.rsmd = this.rs.getMetaData();
			int columnsNumber = this.rsmd.getColumnCount();
			while (this.rs.next()) {
				String[] lineValue = new String[columnsNumber];
				this.results.add(lineValue);
				for (int i = 1; i <= columnsNumber; i++) {
					lineValue[i - 1] = String.valueOf(this.rs.getString(i));
				}
			}
		}

		private void printResults() throws SQLException {
			int columnsNumber = this.rsmd.getColumnCount();
			int[] maxLenArray = new int[columnsNumber - 1];
			for (String[] line : this.results) {
				for (int i = 0; i < columnsNumber - 1; i++) {
					maxLenArray[i] = Math.max(maxLenArray[i], line[i].length());
				}
			}
			for (String[] line : this.results) {
				for (int i = 1; i <= columnsNumber; i++) {
					if (i > 1) {
						System.out.print(";  " + join(" ", "", maxLenArray[i - 2] - line[i - 2].length()));
					}
					System.out.print(rsmd.getColumnName(i) + ": \"" + line[i - 1] + "\"");
				}
				System.out.println("");
			}
		}

		private void discardResults() {
			this.rowCount = this.results.size();
			this.results.clear();
		}
	}

	private final Map<String, List<QueryRunnerResult>> resultMap;
	private final Map<String, Integer> fetchSizes;

	private final String userName, password, url, sid;
	private final int port;
	private final boolean printResults;

	private Connection con;

	/**
	 * Instancia um novo QueryRunner
	 * 
	 * @param userName
	 * @param password
	 * @param url
	 * @param port
	 * @param sid
	 * @param printResults
	 */
	public QueryRunner(String userName, String password, String url, int port, String sid, boolean printResults) {
		System.out.println("Instanciando QueryRunner para a url " + url + " na porta " + port);
		this.userName = userName;
		this.password = password;
		this.url = url;
		this.port = port;
		this.sid = sid;
		this.printResults = printResults;
		this.resultMap = new LinkedHashMap<>();
		this.fetchSizes = new HashMap<>();
	}

	public void connect() {
		this.disconnect();
		System.out.println("Iniciando conexão ao banco de dados");
		try {
			this.con = DriverManager.getConnection("jdbc:oracle:thin:@" + this.url + ":" + this.port + ":" + this.sid,
					this.userName, this.password);
		} catch (SQLException e) {
			System.out.println("Erro ao estabelecer conexão!");
			e.printStackTrace();
			return;
		}
	}

	public void disconnect() {
		try {
			if (this.con != null) {
				System.out.println("Desconectando do banco de dados");
				if (!this.con.isClosed()) {
					this.con.close();
				}
			}
		} catch (SQLException e) {
			System.out.println("Erro ao fechar conexão!");
			e.printStackTrace();
			return;
		}
	}

	private int getFetchSize(String query) {
		if (this.fetchSizes.containsKey(query)) {
			return this.fetchSizes.get(query);
		}
		if (this.con == null) {
			return DEFAULT_FETCH_SIZE;
		}
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select count(1) from (" + query + ")");
			rs.setFetchSize(1);
			int fetchSize = DEFAULT_FETCH_SIZE;
			if (rs.next()) {
				fetchSize = rs.getInt(1);
			}
			if (fetchSize <= 0) {
				fetchSize = 1;
			}
			this.fetchSizes.put(query, fetchSize);
			return fetchSize;
		} catch (SQLException e) {
			try {
				Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				ResultSet rs = stmt.executeQuery(query);
				int fetchSize = DEFAULT_FETCH_SIZE;
				if (rs.last()) {
					fetchSize = rs.getRow();
				}
				if (fetchSize <= 0) {
					fetchSize = 1;
				}
				this.fetchSizes.put(query, fetchSize);
				return fetchSize;
			} catch (SQLException ex) {
				return DEFAULT_FETCH_SIZE;
			}
		}
	}

	/**
	 * Executa uma query N vezes, em que N é o número padrão de repetições nesta
	 * classe.
	 * 
	 * @param query
	 *            A query a ser executada (sem o ponto-e-vírgula final)
	 */
	public void runQuery(String query) {
		this.runQuery(query, DEFAULT_NUM_EXECUTIONS);
	}

	/**
	 * Executa uma query um determinado número de vezes.
	 * 
	 * @param query
	 *            A query a ser executada (sem o ponto-e-vírgula final)
	 * @param numExecutions
	 *            O número de execuções a ser realizado.
	 */
	public void runQuery(String query, int numExecutions) {
		this.runQuery(query, numExecutions, getFetchSize(query));
	}

	/**
	 * Executa uma query um determinado número de vezes, especificando o número de
	 * linhas a ser trazido do banco por transmissão.
	 * 
	 * @param query
	 *            A query a ser executada (sem o ponto-e-vírgula final)
	 * @param numExecutions
	 *            O número de execuções a ser realizado.
	 * @param fetchSize
	 *            O número de linhas a ser trazido a cada ida ao banco.
	 */
	public void runQuery(String query, int numExecutions, int fetchSize) {
		System.out.println("Executando a query (" + numExecutions + " vezes, fetchSize " + fetchSize + "): " + query);
		if (this.con == null) {
			System.out.println("É necessário conectar-se ao banco antes de executar queries");
			return;
		}
		List<QueryRunnerResult> results = resultMap.get(query);
		if (results == null) {
			results = new ArrayList<QueryRunnerResult>(numExecutions);
			resultMap.put(query, results);
		}
		try {
			for (int i = 0; i < numExecutions; i++) {
				Date start = new Date();
				Statement stmt = con.createStatement();
				stmt.setFetchSize(fetchSize);
				ResultSet rs = stmt.executeQuery(query);
				QueryRunnerResult result = new QueryRunnerResult(rs);
				result.fetchResults();
				result.setExecutionTime(start, new Date());
				if (results.isEmpty()) {
					if (printResults) {
						result.printResults();
					}
				} else {
					result.discardResults();
				}
				results.add(result);
			}
		} catch (SQLException e) {
			System.out.println("Erro ao executar query! Query: " + query);
			e.printStackTrace();
		}
	}

	public void printStatistics() {
		for (Entry<String, List<QueryRunnerResult>> entry : resultMap.entrySet()) {
			BigDecimal average = getAvgExecutionTime(entry.getValue());
			BigDecimal variance = getVarianceExecutionTime(entry.getValue(), average);
			BigDecimal stdDeviation = sqrt(variance, EXTENDED_PRECISION);
			System.out.println("");
			System.out.println("---------------------------------------------");
			System.out.println("Query: " + entry.getKey());
			System.out.println("Número de execuções: " + entry.getValue().size());
			System.out.println("Número de linhas obtidas: "
					+ (entry.getValue().isEmpty() ? 0 : entry.getValue().get(0).getRowCount()));
			System.out.println("Tempos (segundos): " + getExecutionTimesString(entry.getValue()));
			System.out.println("Tempo mínimo das execuções: " + getMinExecutionTime(entry.getValue()) + " segundos");
			System.out.println("Tempo médio das execuções: " + average + " segundos");
			System.out.println("Tempo máximo das execuções: " + getMaxExecutionTime(entry.getValue()) + " segundos");
			System.out.println("Variância: " + variance);
			System.out.println("Desvio padrão: " + stdDeviation);
			System.out.println("---------------------------------------------");
		}
	}

	private static BigDecimal divideThousand(BigDecimal bdTime) {
		return bdTime.divide(THOUSAND, STANDARD_PRECISION, RoundingMode.HALF_UP);
	}

	private static BigDecimal getAvgExecutionTime(List<QueryRunnerResult> results) {
		if (results == null || results.isEmpty()) {
			return ZERO;
		}
		long time = 0;
		for (QueryRunnerResult result : results) {
			time += result.getExecutionTime();
		}
		return divideThousand(new BigDecimal(time / results.size()));
	}

	private static BigDecimal getMinExecutionTime(List<QueryRunnerResult> results) {
		if (results == null || results.isEmpty()) {
			return ZERO;
		}
		long time = Long.MAX_VALUE;
		for (QueryRunnerResult result : results) {
			time = Math.min(time, result.getExecutionTime());
		}
		return divideThousand(new BigDecimal(time));
	}

	private static BigDecimal getMaxExecutionTime(List<QueryRunnerResult> results) {
		if (results == null || results.isEmpty()) {
			return ZERO;
		}
		long time = Long.MIN_VALUE;
		for (QueryRunnerResult result : results) {
			time = Math.max(time, result.getExecutionTime());
		}
		return divideThousand(new BigDecimal(time));
	}

	private static BigDecimal getVarianceExecutionTime(List<QueryRunnerResult> results, BigDecimal avg) {
		if (results == null || results.size() <= 1) {
			return ZERO;
		}
		BigDecimal var = BigDecimal.ZERO;
		for (QueryRunnerResult result : results) {
			var = var.add(divideThousand(new BigDecimal(result.getExecutionTime())).subtract(avg).pow(2));
		}
		return var.divide(new BigDecimal(results.size() - 1), EXTENDED_PRECISION, RoundingMode.HALF_UP);
	}

	public static BigDecimal sqrt(BigDecimal num, int scale) {
		BigDecimal x0 = BigDecimal.ZERO;
		BigDecimal x1 = new BigDecimal(Double.toString(Math.sqrt(num.doubleValue())));
		while (x0.compareTo(x1) != 0) {
			x0 = x1;
			x1 = num.divide(x0, EXTENDED_PRECISION, RoundingMode.HALF_UP);
			x1 = x1.add(x0);
			x1 = x1.divide(TWO, EXTENDED_PRECISION, RoundingMode.HALF_UP);
		}
		return x1.setScale(scale, RoundingMode.HALF_UP);
	}

	private static String getExecutionTimesString(List<QueryRunnerResult> results) {
		if (results == null || results.isEmpty()) {
			return "";
		}
		String[] times = new String[results.size()];
		int i = 0;
		for (QueryRunnerResult result : results) {
			times[i++] = divideThousand(new BigDecimal(result.getExecutionTime())).toString();
		}
		return join(times, ", ");
	}

	private static String join(String[] strings, String separator) {
		if (strings == null || strings.length == 0) {
			return "";
		}
		StringBuilder sb = new StringBuilder(strings.length);
		for (int i = 0; i < strings.length; i++) {
			sb.append(strings[i]);
			if (i < strings.length - 1) {
				sb.append(separator);
			}
		}
		return sb.toString();
	}

	private static String join(String str, String separator, int n) {
		if (n <= 0) {
			return "";
		}
		String[] strings = new String[n];
		for (int i = 0; i < strings.length; i++) {
			strings[i] = str;
		}
		return join(strings, separator);
	}

}
