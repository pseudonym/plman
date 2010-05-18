import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;
import java.util.Scanner;
import javax.swing.*;
import javax.swing.table.*;

class GUI extends JFrame {
	private Socket socket;
	private JButton stopButton, startButton, killButton;
	private MyTableModel model;
	private Formatter out;
	private JTable table;

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: java GUI host port");
			System.exit(1);
		}
		final GUI g = new GUI(args[0], Integer.parseInt(args[1]));
		g.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		g.setSize(400,400);
		g.setVisible(true);

		Scanner s = new Scanner(g.socket.getInputStream());
		while (s.hasNextLine()) {
			String l = s.nextLine();
			final String[] a = l.split("\\s+");
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					g.processMessage(a);
				}
			});
		}
		System.out.println("EOF on socket");
		System.exit(0);
	}

	GUI(String host, int port) throws Exception {
		this.setLayout(new BorderLayout());

		this.socket = new Socket(host, port);
		this.out = new Formatter(this.socket.getOutputStream());
		this.out.format("CHELLO\n");
		this.out.flush();

		this.model = new MyTableModel();
		this.table = new JTable(this.model);
		this.table.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		this.add(new JScrollPane(this.table), BorderLayout.CENTER);

		this.stopButton = new JButton("Stop");
		this.stopButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				GUI.this.stop();
			}
		});

		this.startButton = new JButton("Start");
		this.startButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				GUI.this.start();
			}
		});

		this.killButton = new JButton("Kill");
		this.killButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				GUI.this.kill();
			}
		});

		JPanel p = new JPanel(new FlowLayout());
		p.add(this.stopButton);
		p.add(this.killButton);
		p.add(this.startButton);
		this.add(p, BorderLayout.SOUTH);

		this.pack();
	}

	public void processMessage(String[] args) {
		if (!args[0].equals("STATE") || args.length != 4) {
			System.out.println("Unknown message received: " + args);
			return;
		}

		this.model.set(args[1], Integer.parseInt(args[2]), args[3]);
		for (String x : args)
			System.out.print(x + " ");
		System.out.println();
	}

	public void stop() {
		int[] rows = this.table.getSelectedRows();
		for (int x : rows) {
			String host = (String) this.model.getValueAt(x, 0);
			out.format("CSTOP %s\n", host);
		}
		out.flush();
	}

	public void start() {
		int[] rows = this.table.getSelectedRows();
		for (int x : rows) {
			String host = (String) this.model.getValueAt(x, 0);
			out.format("CSTART %s\n", host);
		}
		out.flush();
	}

	public void kill() {
		int[] rows = this.table.getSelectedRows();
		for (int x : rows) {
			String host = (String) this.model.getValueAt(x, 0);
			out.format("CKILL %s\n", host);
		}
		out.flush();
	}

	public static class MyTableModel extends AbstractTableModel {
		private static class Entry implements Comparable<Entry> {
			public String host;
			public int port;
			public String status;

			public int compareTo(Entry e) {
				return this.host.compareTo(e.host);
			}

			public boolean equals(Object o) {
				if (o instanceof Entry)
					return Entry.class.cast(o).host.equals(this.host);
				else
					return false;
			}
		}

		private List<Entry> entries = new ArrayList<Entry>();

		public void set(String host, int port, String status) {
			Entry e = new Entry();
			e.host = host;
			e.port = port;
			e.status = status;
			this.entries.remove(e);
			this.entries.add(e);
			Collections.sort(this.entries);
			fireTableDataChanged();
		}

		public int getColumnCount() {
			return 3;
		}

		public int getRowCount() {
			return entries.size();
		}

		public Object getValueAt(int row, int col) {
			switch (col) {
			case 0:
				return entries.get(row).host;
			case 1:
				return entries.get(row).port;
			case 2:
				return entries.get(row).status;
			default:
				return null;
			}
		}

		public String getColumnName(int col) {
			switch (col) {
			case 0:
				return "Host";
			case 1:
				return "Port";
			case 2:
				return "Status";
			default:
				return null;
			}
		}

		public Class<?> getColumnClass(int col) {
			switch (col) {
			case 0:
				return String.class;
			case 1:
				return Integer.class;
			case 2:
				return String.class;
			default:
				return null;
			}
		}
	}
}
