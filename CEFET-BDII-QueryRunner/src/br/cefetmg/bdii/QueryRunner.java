package br.cefetmg.bdii;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class QueryRunner {

	public static class QueryRunnerResult {
		private final ResultSet rs;
		private ResultSetMetaData rsmd;
		private final List<List<String>> results;
		private long executionTime;

		private QueryRunnerResult(ResultSet resultSet) {
			this.rs = resultSet;
			this.results = new ArrayList<List<String>>();
			this.executionTime = 0L;
		}

		private long getExecutionTime() {
			return this.executionTime;
		}

		private void setExecutionTime(Date inicio, Date fim) {
			this.executionTime = Math.abs(fim.getTime() - inicio.getTime());
		}

		public int getRowCount() {
			return results.size();
		}

		private void fetchResults() throws SQLException {
			this.results.clear();
			this.rsmd = this.rs.getMetaData();
			int columnsNumber = this.rsmd.getColumnCount();
			while (this.rs.next()) {
				List<String> lineValue = new ArrayList<String>(columnsNumber);
				this.results.add(lineValue);
				for (int i = 1; i <= columnsNumber; i++) {
					lineValue.add(this.rs.getString(i));
				}
			}
		}

		private void printResults() throws SQLException {
			int columnsNumber = this.rsmd.getColumnCount();
			for (List<String> line : this.results) {
				for (int i = 1; i <= columnsNumber; i++) {
					if (i > 1) {
						System.out.print(",  ");
					}
					System.out.print(line.get(i) + " " + rsmd.getColumnName(i));
				}
				System.out.println("");
			}
		}

		private void discardResults() {
			for (List<String> row : results) {
				row.clear();
			}
		}
	}

	private final Map<String, List<QueryRunnerResult>> resultMap;

	private final String userName, password, url, sid;
	private final int port;
	private final boolean printMode;

	private Connection con;

	/**
	 * Instancia um novo QueryRunner
	 * 
	 * @param userName
	 * @param password
	 * @param url
	 * @param port
	 * @param sid
	 * @param printMode
	 */
	public QueryRunner(String userName, String password, String url, int port, String sid, boolean printMode) {
		this.userName = userName;
		this.password = password;
		this.url = url;
		this.port = port;
		this.sid = sid;
		this.printMode = printMode;
		this.resultMap = new LinkedHashMap<String, List<QueryRunnerResult>>();
	}

	public void connect() {
		this.disconnect();
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
		if (this.con != null) {
			try {
				this.con.close();
			} catch (SQLException e) {
				System.out.println("Erro ao fechar conexão anterior!");
				e.printStackTrace();
				return;
			}
		}
	}

	public void runQuery(String query, int numExecutions) {
		if (con == null) {
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
				ResultSet rs = stmt.executeQuery(query);
				QueryRunnerResult result = new QueryRunnerResult(rs);
				result.fetchResults();
				if (results.isEmpty()) {
					if (printMode) {
						result.printResults();
					}
				} else {
					result.discardResults();
				}
				result.setExecutionTime(start, new Date());
				results.add(result);
			}
		} catch (SQLException e) {
			System.out.println("Erro ao executar query!");
			e.printStackTrace();
		}
	}

	public long getAverageExecutionTime(List<QueryRunnerResult> results) {
		long time = 0;
		for (QueryRunnerResult result : results) {
			time += result.getExecutionTime();
		}
		return time / results.size();
	}

	public void printStatistics() {
		for (Entry<String, List<QueryRunnerResult>> entry : resultMap.entrySet()) {
			System.out.println("");
			System.out.println("---------------------------------------------");
			System.out.println("Query: " + entry.getKey());
			System.out.println("Número de execuções: " + entry.getValue().size());
			System.out.println("Número de linhas obtidas: "
					+ (entry.getValue().isEmpty() ? 0 : entry.getValue().get(0).getRowCount()));
			System.out.println("Tempo médio das execuções: " + getAverageExecutionTime(entry.getValue()));
			System.out.println("---------------------------------------------");
		}
	}

}
