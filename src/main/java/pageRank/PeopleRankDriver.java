package pageRank;

import java.io.IOException;


public class PeopleRankDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        int sumCount = 4;
        // 生成概率矩阵
        AdjacencyMatrix.run("/common/page.txt","/data/scc/Adjacency/output", sumCount);
        for (int i = 0; i < 10; i++) {
            // 2.迭代
        	CalcPageRank.run("pageRank.txt","part-r-00000","/data/scc/Adjacency/output2",sumCount);
        }
        // 标准化
        FinallyResult.run("/data/scc/Adjacency/pageRank.txt","/data/scc/Adjacency/standardization.txt");
    }
}
