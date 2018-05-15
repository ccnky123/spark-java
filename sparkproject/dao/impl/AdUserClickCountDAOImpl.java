package com.ibeifeng.sparkproject.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.ibeifeng.sparkproject.dao.IAdUserClickCountDAO;
import com.ibeifeng.sparkproject.domain.AdUserClickCount;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;
import com.ibeifeng.sparkproject.model.AdUserClickCountQueryResult;

/**
 * customer advertisement click count
 * @author Administrator
 *
 */
public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO {

	@Override
	public void updateBatch(List<AdUserClickCount> adUserClickCounts) {
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		// Seperate different type ad
		List<AdUserClickCount> insertAdUserClickCounts = new ArrayList<AdUserClickCount>();
		List<AdUserClickCount> updateAdUserClickCounts = new ArrayList<AdUserClickCount>();
		
		String selectSQL = "SELECT count(*) FROM ad_user_click_count "
				+ "WHERE date=? AND user_id=? AND ad_id=? ";
		Object[] selectParams = null;
		
		for(AdUserClickCount adUserClickCount : adUserClickCounts) {
			final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();
			
			selectParams = new Object[]{adUserClickCount.getDate(), 
					adUserClickCount.getUserid(), adUserClickCount.getAdid()};
			
			jdbcHelper.executeQuery(selectSQL, selectParams, new JDBCHelper.QueryCallback() {
				
				@Override
				public void process(ResultSet rs) throws Exception {
					if(rs.next()) {
						int count = rs.getInt(1);
						queryResult.setCount(count);  
					}
				}
			});
			
			int count = queryResult.getCount();
			
			if(count > 0) {
				updateAdUserClickCounts.add(adUserClickCount);
			} else {
				insertAdUserClickCounts.add(adUserClickCount);
			}
		}
		
		String insertSQL = "INSERT INTO ad_user_click_count VALUES(?,?,?,?)";  
		List<Object[]> insertParamsList = new ArrayList<Object[]>();
		
		for(AdUserClickCount adUserClickCount : insertAdUserClickCounts) {
			Object[] insertParams = new Object[]{adUserClickCount.getDate(),
					adUserClickCount.getUserid(),
					adUserClickCount.getAdid(),
					adUserClickCount.getClickCount()};  
			insertParamsList.add(insertParams);
		}
		
		jdbcHelper.executeBatch(insertSQL, insertParamsList);
		
		
		String updateSQL = "UPDATE ad_user_click_count SET click_count=click_count+? "
				+ "WHERE date=? AND user_id=? AND ad_id=? ";  
		List<Object[]> updateParamsList = new ArrayList<Object[]>();
		
		for(AdUserClickCount adUserClickCount : updateAdUserClickCounts) {
			Object[] updateParams = new Object[]{adUserClickCount.getClickCount(),
					adUserClickCount.getDate(),
					adUserClickCount.getUserid(),
					adUserClickCount.getAdid()};  
			updateParamsList.add(updateParams);
		}
		
		jdbcHelper.executeBatch(updateSQL, updateParamsList);
	}
	
	/**
	 * 根
	 * @param date 
	 * @param userid
	 * @param adid 
	 * @return
	 */
	public int findClickCountByMultiKey(String date, long userid, long adid) {
		String sql = "SELECT click_count "
				+ "FROM ad_user_click_count "
				+ "WHERE date=? "
				+ "AND user_id=? "
				+ "AND ad_id=?";
		
		Object[] params = new Object[]{date, userid, adid};
		
		final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {  
			
			@Override
			public void process(ResultSet rs) throws Exception {
				if(rs.next()) {
					int clickCount = rs.getInt(1);
					queryResult.setClickCount(clickCount); 
				}
			}
		});
		
		int clickCount = queryResult.getClickCount();
		
		return clickCount;
	}

}
