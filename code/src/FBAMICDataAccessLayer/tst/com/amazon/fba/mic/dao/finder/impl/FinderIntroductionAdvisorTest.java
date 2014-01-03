package com.amazon.fba.mic.dao.finder.impl;

import static org.easymock.EasyMock.createMockBuilder;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.amazon.fba.mic.dao.impl.GenericDaoImpl;

public class FinderIntroductionAdvisorTest {
    private FinderIntroductionAdvisor<?> advisor;
    
    @Before
    public void setUp() throws Exception {
        advisor = createMockBuilder(FinderIntroductionAdvisor.class).createMock();
    }
    
    @Test
    public void testFinderIntroductionAdvisor() {
        assertNotNull(new FinderIntroductionAdvisor<Object>());
    }
    
    @Test
    public void testMatchesClass() {
        assertTrue(advisor.matches(GenericDaoImpl.class));
        assertTrue(advisor.matches(Object.class) == false);
    }
    
    

}
