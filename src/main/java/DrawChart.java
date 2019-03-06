import java.awt.*;
import java.util.ArrayList;
import java.util.List;
import javax.swing.JFrame;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

public class DrawChart extends JFrame {
    private static final long serialVersionUID = 1L;
    private float count1;
    private float count2;
    private float count3;
    private int time;
    private List<Float[]> history;

    public DrawChart(String title, float count1, float count2, float count3, int time) {
        super(title);
        this.time = time;
        this.count1 = count1;
        this.count2 = count2;
        this.count3 = count3;
        Float[] values = {count1, count2, count3};
        history = new ArrayList<Float[]>();
        history.add(values);
        // Create dataset
        XYDataset dataset = createDataset();

        // Create chart
        JFreeChart chart = ChartFactory.createXYLineChart(
                "Sentiment trend", // Chart title
                "Time", // X-Axis Label
                "Trend", // Y-Axis Label
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false);
        ChartPanel panel = new ChartPanel(chart);
        setContentPane(panel);

        //customization
        XYPlot plot = chart.getXYPlot();
        customize(plot);
    }

    public void update(float count1, float count2, float count3, int time) {
        this.time = time;
        this.count1 = count1;
        this.count2 = count2;
        this.count3 = count3;
        Float[] values = {count1, count2, count3};
        history.add(values);
        // Create dataset
        XYDataset dataset = createDataset();
        // Create chart
        JFreeChart chart = ChartFactory.createXYLineChart(
                "Sentiment trend", // Chart title
                "Time", // X-Axis Label
                "Trend", // Y-Axis Label
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false);
        ChartPanel panel = new ChartPanel(chart);
        setContentPane(panel);

        //customization
        XYPlot plot = chart.getXYPlot();
        customize(plot);
    }

    private XYDataset createDataset() {

        XYSeriesCollection dataset = new XYSeriesCollection();

        XYSeries series = new XYSeries(utils.Utils.getKeywords().get(0));
        if (history.size() > 1) {
            for (int i = 0; i < history.size(); i++) {
                series.add(i * 10, history.get(i)[0]);
            }
        }
        series.add(time, count1); //add(x value, y value)

        dataset.addSeries(series);

        XYSeries series1 = new XYSeries(utils.Utils.getKeywords().get(1));
        if (history.size() > 1) {
            for (int i = 0; i < history.size(); i++) {
                series1.add(i * 10, history.get(i)[1]);
            }
        }
        series1.add(time, count2);
        //Add series to dataset
        dataset.addSeries(series1);

        XYSeries series2 = new XYSeries(utils.Utils.getKeywords().get(2));
        if (history.size() > 1) {
            for (int i = 0; i < history.size(); i++) {
                series2.add(i * 10, history.get(i)[2]);
            }
        }
        series2.add(time, count3);
        dataset.addSeries(series2);

        return dataset;
    }

    private void customize(XYPlot plot) {
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        renderer.setSeriesPaint(0, Color.RED);
        renderer.setSeriesStroke(0, new BasicStroke(2.0f));

        renderer.setSeriesPaint(1, Color.BLUE);
        renderer.setSeriesStroke(1, new BasicStroke(2.0f));

        renderer.setSeriesPaint(2, Color.GREEN);
        renderer.setSeriesStroke(2, new BasicStroke(2.0f));

        plot.setRenderer(renderer);

        plot.setRangeGridlinesVisible(false);
        plot.setDomainGridlinesVisible(false);

        plot.setBackgroundPaint(Color.white);
    }
}