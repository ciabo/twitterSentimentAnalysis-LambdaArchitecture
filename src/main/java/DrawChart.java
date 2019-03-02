import java.util.ArrayList;
import javax.swing.JFrame;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

public class DrawChart extends JFrame {
    private static final long serialVersionUID = 1L;
    private ArrayList<Double> solution;
    private ArrayList<Double> solution1;

    public DrawChart(String title, ArrayList<Double> sol, ArrayList<Double> sol1) {
        super(title);
        this.solution = sol;
        this.solution1 = sol1;
        // Create dataset
        XYDataset dataset = createDataset();
        // Create chart
        JFreeChart chart = ChartFactory.createXYLineChart(
                "Industrial Process Cost", // Chart title
                "Time", // X-Axis Label
                "Cost", // Y-Axis Label
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false);
        ChartPanel panel = new ChartPanel(chart);
        setContentPane(panel);
    }

    private XYDataset createDataset() {

        XYSeriesCollection dataset = new XYSeriesCollection();

        XYSeries series = new XYSeries("Cost at 06:00 ");
        for (int t = 0; t < solution.size(); t++) {
            series.add(t, solution.get(t));
        }

        dataset.addSeries(series);

        XYSeries series1 = new XYSeries("Cost at 17:00");
        for (int t = 0; t < solution1.size(); t++) {
            series1.add(t, solution1.get(t));
        }

        //Add series to dataset
        dataset.addSeries(series1);

        return dataset;
    }
}