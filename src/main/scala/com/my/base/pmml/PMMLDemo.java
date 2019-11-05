package com.my.base.pmml;


import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.*;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * @Author: Yuan Liu
 * @Description:
 * @Date: Created in 9:55 2019/8/21
 * <p>
 * Good Good Study Day Day Up
 */
public class PMMLDemo {


    private Evaluator loadPmml() {
        PMML pmml = new PMML();
        InputStream inputStream = null;
        try {
            /**
             * 需要引用python保存下来的pmml文件
             */
            inputStream = new FileInputStream("E:\\workspace\\MachineLearning\\模型存储\\lrHeart.xml");
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (inputStream == null) {
            return null;
        }
        InputStream input = inputStream;
        try {
            // org.jpmml.pmml-model 包下的
            pmml = org.jpmml.model.PMMLUtil.unmarshal(input);
        } catch (SAXException e1) {
            e1.printStackTrace();
        } catch (JAXBException e1) {
            e1.printStackTrace();
        } finally {
            //关闭输入流
            try {
                input.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
        Evaluator evaluator = modelEvaluatorFactory.newModelEvaluator(pmml);
        pmml = null;
        return evaluator;
    }


    private int predict(Evaluator evaluator, int a, int b, int c, int d) {
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("sbp", 160);
        data.put("tobacco", 12);
        data.put("ldl", 5.73);
        data.put("adiposity", 23.11);
        data.put("famhist", "Present");
        data.put("typea", 49);
        data.put("obesity", 25.3);
        data.put("alcohol", 97.2);
        data.put("age", 52);

        List<InputField> inputFields = evaluator.getInputFields();
        //过模型的原始特征，从画像中获取数据，作为模型输入
        Map<FieldName, FieldValue> arguments = new LinkedHashMap<FieldName, FieldValue>();
        for (InputField inputField : inputFields) {
            FieldName inputFieldName = inputField.getName();
            Object rawValue = data.get(inputFieldName.getValue());
            FieldValue inputFieldValue = inputField.prepare(rawValue);
            arguments.put(inputFieldName, inputFieldValue);
        }

        Map<FieldName, ?> results = evaluator.evaluate(arguments);
        List<TargetField> targetFields = evaluator.getTargetFields();

        TargetField targetField = targetFields.get(0);
        FieldName targetFieldName = targetField.getName();

        Object targetFieldValue = results.get(targetFieldName);
        System.out.println("target: " + targetFieldName.getValue() + " value: " + targetFieldValue);
        int primitiveValue = -1;
        if (targetFieldValue instanceof Computable) {
            Computable computable = (Computable) targetFieldValue;
            primitiveValue = (Integer) computable.getResult();
        }
        System.out.println(a + " " + b + " " + c + " " + d + ":" + primitiveValue);
        return primitiveValue;
    }


    public static void main(String args[]) {
        PMMLDemo demo = new PMMLDemo();
        Evaluator model = demo.loadPmml();
        demo.predict(model, 1, 8, 99, 1);           // 填充数据即可预测
    }

}
