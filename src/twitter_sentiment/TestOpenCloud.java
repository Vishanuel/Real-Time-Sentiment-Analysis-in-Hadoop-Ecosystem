package twitter_sentiment;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;
import javax.swing.Timer;

import org.mcavallo.opencloud.Cloud;
import org.mcavallo.opencloud.Tag;

import javafx.application.Platform;


public class TestOpenCloud {


    Connection con;
    Statement stmt;
    
    protected void initUI() {
    	
        JFrame frame = new JFrame("Postive sentiment word cloud for Amazon");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        final JPanel panel = new JPanel(new WrapLayout());
        Cloud cloud = new Cloud();
        
        JScrollPane scrollPane = new JScrollPane(panel);
        scrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
        scrollPane.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
        frame.add(scrollPane);
        frame.setSize(900, 900);
        frame.setVisible(true);
        
        JFrame framebay = new JFrame("Postive sentiment word cloud for eBay");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        final JPanel panelbay = new JPanel(new WrapLayout());
        Cloud cloud1 = new Cloud();
        
        JScrollPane scrollPane1 = new JScrollPane(panelbay);
        scrollPane1.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
        scrollPane1.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
        framebay.add(scrollPane1);
        framebay.setSize(900, 900);
        framebay.setVisible(true);
        
        JFrame framealiexpress = new JFrame("Postive sentiment word cloud for Aliexpress");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        final JPanel panelaliexpress = new JPanel(new WrapLayout());
        Cloud cloud2 = new Cloud();
        
        JScrollPane scrollPane2 = new JScrollPane(panelaliexpress);
        scrollPane2.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
        scrollPane2.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
        framealiexpress.add(scrollPane2);
        framealiexpress.setSize(900, 900);
        framealiexpress.setVisible(true);
               
        try {
		con = DriverManager.getConnection("jdbc:hive2://localhost:10000/", "", "");
		int fontSize = 10;
		int count=0;
		stmt = con.createStatement();
		ResultSet rs2 = stmt.executeQuery("select count(id),search from word_join where rating IS NOT NULL AND search IS NOT NULL AND search = 'amazon' AND rating > 0 GROUP by search");
		while (rs2.next()) {
			count = rs2.getInt("_c0");
		}
		
		String words[] = new String[count];
		
        ResultSet rs = stmt.executeQuery("select * from word_join where rating IS NOT NULL AND search IS NOT NULL AND search = 'amazon' AND rating > 0 ORDER BY word, rating");
        int i = 0;
        while (rs.next()) {
        	
            int rating = rs.getInt("rating");
            String search = rs.getString("search");
            String word = rs.getString("word");
            
            if(search != null && word != null) {
	          	search.strip();
	          	word.strip();
	             //Update the chart
	          	words[i] = word; 
	          	if(i != 0) {
		          	if(words[i-1].contentEquals(word)) {
		          		fontSize = fontSize + 2;
		          	}
		          	else{
		          		cloud.addTag(word);
		          		JLabel label = new JLabel(words[i-1]);
		          		label.setFont(label.getFont().deriveFont((float) fontSize));

		                label.setOpaque(false);
					            
				            
				        panel.add(label);
				        panel.revalidate();
				        panel.repaint();

		                fontSize = 10;
		          		
		          	}
	          	}
            }
              i++;
             System.out.println("    "+search);
          }
        
        int fontSize1 = 10;
		int count1=0;
		stmt = con.createStatement();
		ResultSet rs3 = stmt.executeQuery("select count(id),search from word_join where rating IS NOT NULL AND search IS NOT NULL AND search = 'amazon' AND rating > 0 GROUP by search");
		while (rs3.next()) {
			count1 = rs3.getInt("_c0");
		}
		
		String words1[] = new String[count];
		
        ResultSet rs4 = stmt.executeQuery("select * from word_join where rating IS NOT NULL AND search IS NOT NULL AND search = 'eBay' AND rating > 0 ORDER BY word, rating");
         i = 0;
        while (rs4.next()) {
        	
            int rating = rs4.getInt("rating");
            String search = rs4.getString("search");
            String word = rs4.getString("word");
            
            if(search != null && word != null) {
	          	search.strip();
	          	word.strip();
	             //Update the chart
	          	words1[i] = word; 
	          	if(i != 0) {
		          	if(words1[i-1].contentEquals(word)) {
		          		fontSize1 = fontSize1 + 2;
		          	}
		          	else{
		          		cloud1.addTag(word);
		          		JLabel label = new JLabel(words1[i-1]);
		          		label.setFont(label.getFont().deriveFont((float) fontSize1));

		                label.setOpaque(false);
					            
				            
		                panelbay.add(label);
		                panelbay.revalidate();
		                panelbay.repaint();

		                fontSize1 = 10;
		          		
		          	}
	          	}
            }
              i++;
             System.out.println("    "+search);
          }
        
        int fontSize2 = 10;
		int count2=0;
		stmt = con.createStatement();
		ResultSet rs5 = stmt.executeQuery("select count(id),search from word_join where rating IS NOT NULL AND search IS NOT NULL AND search = 'amazon' AND rating > 0 GROUP by search");
		while (rs5.next()) {
			count2 = rs5.getInt("_c0");
		}
		
		String words2[] = new String[count];
		
        ResultSet rs6 = stmt.executeQuery("select * from word_join where rating IS NOT NULL AND search IS NOT NULL AND search = 'aliexpress' AND rating > 0 ORDER BY word, rating");
        i = 0;
        while (rs.next()) {
        	
            int rating = rs6.getInt("rating");
            String search = rs6.getString("search");
            String word = rs6.getString("word");
            
            if(search != null && word != null) {
	          	search.strip();
	          	word.strip();
	             //Update the chart
	          	words2[i] = word; 
	          	if(i != 0) {
		          	if(words2[i-1].contentEquals(word)) {
		          		fontSize2 = fontSize2 + 2;
		          	}
		          	else{
		          		cloud2.addTag(word);
		          		JLabel label = new JLabel(words2[i-1]);
		          		label.setFont(label.getFont().deriveFont((float) fontSize2));

		                label.setOpaque(false);
					            
				            
				        panelaliexpress.add(label);
				        panelaliexpress.revalidate();
				        panelaliexpress.repaint();

		                fontSize2 = 10;
		          		
		          	}
	          	}
            }
              i++;
             System.out.println("    "+search);
          }
        
        
        } catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        System.out.println("Complete");
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                new TestOpenCloud().initUI();
            }
        });
    }

    public class WrapLayout extends FlowLayout {
        private Dimension preferredLayoutSize;

        /**
         * Constructs a new <code>WrapLayout</code> with a left alignment and a default 5-unit horizontal and vertical gap.
         */
        public WrapLayout() {
            super();
        }

        /**
         * Constructs a new <code>FlowLayout</code> with the specified alignment and a default 5-unit horizontal and vertical gap. The value
         * of the alignment argument must be one of <code>WrapLayout</code>, <code>WrapLayout</code>, or <code>WrapLayout</code>.
         * 
         * @param align
         *            the alignment value
         */
        public WrapLayout(int align) {
            super(align);
        }

        /**
         * Creates a new flow layout manager with the indicated alignment and the indicated horizontal and vertical gaps.
         * <p>
         * The value of the alignment argument must be one of <code>WrapLayout</code>, <code>WrapLayout</code>, or <code>WrapLayout</code>.
         * 
         * @param align
         *            the alignment value
         * @param hgap
         *            the horizontal gap between components
         * @param vgap
         *            the vertical gap between components
         */
        public WrapLayout(int align, int hgap, int vgap) {
            super(align, hgap, vgap);
        }

        /**
         * Returns the preferred dimensions for this layout given the <i>visible</i> components in the specified target container.
         * 
         * @param target
         *            the component which needs to be laid out
         * @return the preferred dimensions to lay out the subcomponents of the specified container
         */
        @Override
        public Dimension preferredLayoutSize(Container target) {
            return layoutSize(target, true);
        }

        /**
         * Returns the minimum dimensions needed to layout the <i>visible</i> components contained in the specified target container.
         * 
         * @param target
         *            the component which needs to be laid out
         * @return the minimum dimensions to lay out the subcomponents of the specified container
         */
        @Override
        public Dimension minimumLayoutSize(Container target) {
            Dimension minimum = layoutSize(target, false);
            minimum.width -= getHgap() + 1;
            return minimum;
        }

        /**
         * Returns the minimum or preferred dimension needed to layout the target container.
         * 
         * @param target
         *            target to get layout size for
         * @param preferred
         *            should preferred size be calculated
         * @return the dimension to layout the target container
         */
        private Dimension layoutSize(Container target, boolean preferred) {
            synchronized (target.getTreeLock()) {
                // Each row must fit with the width allocated to the containter.
                // When the container width = 0, the preferred width of the container
                // has not yet been calculated so lets ask for the maximum.

                int targetWidth = target.getSize().width;

                if (targetWidth == 0) {
                    targetWidth = Integer.MAX_VALUE;
                }

                int hgap = getHgap();
                int vgap = getVgap();
                Insets insets = target.getInsets();
                int horizontalInsetsAndGap = insets.left + insets.right + hgap * 2;
                int maxWidth = targetWidth - horizontalInsetsAndGap;

                // Fit components into the allowed width

                Dimension dim = new Dimension(0, 0);
                int rowWidth = 0;
                int rowHeight = 0;

                int nmembers = target.getComponentCount();

                for (int i = 0; i < nmembers; i++) {
                    Component m = target.getComponent(i);

                    if (m.isVisible()) {
                        Dimension d = preferred ? m.getPreferredSize() : m.getMinimumSize();

                        // Can't add the component to current row. Start a new row.

                        if (rowWidth + d.width > maxWidth) {
                            addRow(dim, rowWidth, rowHeight);
                            rowWidth = 0;
                            rowHeight = 0;
                        }

                        // Add a horizontal gap for all components after the first

                        if (rowWidth != 0) {
                            rowWidth += hgap;
                        }

                        rowWidth += d.width;
                        rowHeight = Math.max(rowHeight, d.height);
                    }
                }

                addRow(dim, rowWidth, rowHeight);

                dim.width += horizontalInsetsAndGap;
                dim.height += insets.top + insets.bottom + vgap * 2;

                // When using a scroll pane or the DecoratedLookAndFeel we need to
                // make sure the preferred size is less than the size of the
                // target container so shrinking the container size works
                // correctly. Removing the horizontal gap is an easy way to do this.

                Container scrollPane = SwingUtilities.getAncestorOfClass(JScrollPane.class, target);

                if (scrollPane != null) {
                    dim.width -= hgap + 1;
                }

                return dim;
            }
        }

        /*
         *  A new row has been completed. Use the dimensions of this row
         *  to update the preferred size for the container.
         *
         *  @param dim update the width and height when appropriate
         *  @param rowWidth the width of the row to add
         *  @param rowHeight the height of the row to add
         */
        private void addRow(Dimension dim, int rowWidth, int rowHeight) {
            dim.width = Math.max(dim.width, rowWidth);

            if (dim.height > 0) {
                dim.height += getVgap();
            }

            dim.height += rowHeight;
        }
    }
}