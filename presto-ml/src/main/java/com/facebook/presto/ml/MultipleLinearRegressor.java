/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.ml;

import com.facebook.presto.ml.type.ModelType;
import com.google.common.primitives.Doubles;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.ml.type.RegressorType.REGRESSOR;

public class MultipleLinearRegressor
        implements Regressor
{
    double[] para;

    public MultipleLinearRegressor()
    {
        this.para = null;
    }

    public MultipleLinearRegressor(double[] param)
    {
        this.para = param;
    }

    public static MultipleLinearRegressor deserialize(byte[] modelData)
    {
        ByteBuffer paramBB = ByteBuffer.wrap(modelData);
        double[] param = new double[modelData.length / 8];
        for (int i = 0; i < param.length; i++) {
            param[i] = paramBB.getDouble();
        }

        return new MultipleLinearRegressor(param);
    }

    @Override
    public double regress(FeatureVector features)
    {
        List<Double> featureList = new ArrayList<>(features.getFeatures().values());
        double result = para[0];
        for (int i = 1; i < para.length; i++) {
            result += para[i] * featureList.get(i - 1);
        }

        return result;
    }

    @Override
    public ModelType getType()
    {
        return REGRESSOR;
    }

    @Override
    public byte[] getSerializedData()
    {
        ByteBuffer paramBB = ByteBuffer.allocate(para.length * 8);
        for (double param : para) {
            paramBB.putDouble(param);
        }
        return paramBB.array();
    }

    @Override
    public void train(Dataset dataset)
    {
        OLSMultipleLinearRegression olsm = new OLSMultipleLinearRegression();
        List<FeatureVector> featureVectors = dataset.getDatapoints();
        int numDatapoints = featureVectors.size();
        int featureSize = featureVectors.get(0).getFeatures().size();
        double[][] featureValues = new double[numDatapoints][featureSize];
        for (int i = 0; i < numDatapoints; i++) {
            List<Double> featureValueList = new ArrayList<>(featureVectors.get(i).getFeatures().values());
            for (int j = 0; j < featureSize; j++) {
                featureValues[i][j] = featureValueList.get(j);
            }
        }

        olsm.newSampleData(Doubles.toArray(dataset.getLabels()), featureValues);
        para = olsm.estimateRegressionParameters();
    }
}
