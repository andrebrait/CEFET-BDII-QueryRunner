package br.cefetmg.bdii;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.mozilla.universalchardet.UniversalDetector;

public class QueryUtils {

	private static final String SQL_REGEX = "^\\s*?SELECT[\\s]+?[\\s\\S]+?\\;\\s*?$";

	private static Charset detectCharSet(String caminho) {
		try {
			byte[] buf = new byte[4096];
			FileInputStream fis = new FileInputStream(caminho);

			UniversalDetector detector = new UniversalDetector(null);

			int nread;
			while ((nread = fis.read(buf)) > 0 && !detector.isDone()) {
				detector.handleData(buf, 0, nread);
			}
			detector.dataEnd();

			String encoding = detector.getDetectedCharset();

			detector.reset();
			fis.close();

			return Charset.forName(encoding);
		} catch (IllegalArgumentException | IOException e) {
			return Charset.defaultCharset();
		}
	}

	/**
	 * Retorna uma lista de Strings com queries que foram obtidas a partir do
	 * arquivo referenciado em 'caminho'.
	 * 
	 * @param caminho
	 *            Path do arquivo, devidamente escapado (exemplo:
	 *            "C:\Users\fulano\queriescaminho.sql" ou
	 *            "/home/fulano/queries.sql")
	 * @return Vetor de Strings com as queries.
	 */
	public static List<String> parse(String caminho) {
		Path path = Paths.get(caminho);
		return parse(path);
	}

	/**
	 * Retorna uma lista de Strings com queries que foram obtidas a partir do
	 * arquivo referenciado em 'path'.
	 * 
	 * @param path
	 *            Path do arquivo, devidamente escapado (exemplo:
	 *            "C:\Users\fulano\queriescaminho.sql" ou
	 *            "/home/fulano/queries.sql")
	 * @return Vetor de Strings com as queries.
	 */
	public static List<String> parse(Path path) {
		try {
			String wholeFile = new String(Files.readAllBytes(path), detectCharSet(path.toString()));
			Pattern p = Pattern.compile(SQL_REGEX, Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
			Matcher m = p.matcher(wholeFile);
			List<String> queries = new ArrayList<>();
			while (m.find()) {
				String query = wholeFile.substring(m.start(), m.end());
				queries.add(query.replaceAll("[\n\r]+", " ").trim().replaceFirst(";$", ""));
			}
			return queries;
		} catch (IOException | InvalidPathException e) {
			System.out.println("Erro ao ler arquivo! Arquivo: " + path.toString());
			e.printStackTrace();
			return new ArrayList<>();
		}
	}

}
