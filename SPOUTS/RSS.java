package com.sinfonier.spouts;

import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.sinfonier.util.ComponentType;
import com.sinfonier.util.SinfonierUtils;
import com.sinfonier.util.XMLProperties;

//@formatter:off
/**
* Feed RSS Spout. Retrieve data from feed RSS.
* <p> XML Options:<br/>
* <ul>
* <li> <b>{@code <sources> <source> <sourceId></sourceId> <grouping field="field"></grouping> </source> ... </sources>}</b> - Needed. Sources where this bolt must receive tuples. </li>
* <li> <b>{@code <url></url>}</b> - Needed. Url from RSS you want retrieve. </li>
* <li> <b>{@code <frequency></frequency>}</b> - Needed.  </li>
* <li> <b>{@code <paralellism>1</paralellism> }</b> - Needed. Parallelism. </li>
* </ul>
*/
//@formatter:on
public class RSS extends BaseRichSpout {

    private String xmlPath;
    private String spoutName;
    private XMLProperties xml;
    private SpoutOutputCollector _collector;
    private XmlMapper xmlMapper;

    private String entity = "rssItem";
    private URL url;

    private LinkedBlockingQueue<String> queue = null;
    private int frequency;

    public RSS(String spoutName, String xmlPath) {
        this.xmlPath = xmlPath;
        this.spoutName = spoutName;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        xml = new XMLProperties(spoutName, ComponentType.SPOUT, xmlPath);
        xmlMapper = new XmlMapper();
        SinfonierUtils.broadcastWorker((String) conf.get(Config.TOPOLOGY_NAME), context);
        _collector = collector;
        
        try {
                url = new URL(xml.get("url", true));
            } catch (Exception e) {
                e.printStackTrace();
            }

        queue = new LinkedBlockingQueue<String>(1000);
        frequency = xml.getInt("frequency", true);

        JobDetail job = JobBuilder.newJob(RetrieveAndParseFeed.class)
                .withIdentity("dummyJobName", "group1").build();

        Trigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity("Retrieve Items")
                .withSchedule(
                        SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(frequency)
                                .repeatForever()).build();

        Scheduler scheduler;
        try {
                scheduler = new StdSchedulerFactory().getScheduler();
                scheduler.getContext().put("queue", queue);
                scheduler.getContext().put("url", url);
                scheduler.start();
                scheduler.scheduleJob(job, trigger);
            } 
        catch (SchedulerException e) 
            {
                e.printStackTrace();
            }
    }

    @Override
    public void nextTuple() {
        if (!queue.isEmpty()) 
            {
                String json = queue.poll();
                _collector.emit(new Values(entity, json));
            } 
        else 
            {
                Utils.sleep(50);
            }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
        declarer.declare(new Fields("entity", "map"));
    }

    private class RetrieveAndParseFeed implements Job {

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            SchedulerContext schedulerContext = null;
            URL url;
            try 
            {
                schedulerContext = context.getScheduler().getContext();
                @SuppressWarnings("unchecked")
                LinkedBlockingQueue<String> queue = (LinkedBlockingQueue<String>) schedulerContext
                        .get("queue");
                url = (URL) schedulerContext.get("url");
                InputStream is = url.openStream();
                int ptr = 0;
                StringBuilder builder = new StringBuilder();
                while ((ptr = is.read()) != -1) {
                    builder.append((char) ptr);
                }
                String xml = builder.toString();

                JSONObject jsonObject = XML.toJSONObject(xml).getJSONObject("rss")
                        .getJSONObject("channel");
                JSONArray jsonArray = jsonObject.getJSONArray("item");
                for (int i = 0; i < jsonArray.length(); i++) {
                    queue.put(jsonArray.getJSONObject(i).toString());
                }
            } 
            catch (SchedulerException e1) 
            {
                e1.printStackTrace();
            } 
            catch (Exception e) 
            {
                e.printStackTrace();
            }
        }
    }
}