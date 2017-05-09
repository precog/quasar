/*
 * Copyright 2014–2017 SlamData Inc.
 *
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

package quasar.mimir

import quasar.blueeyes._
import quasar.precog.common._
import quasar.precog.util.Identifier
import quasar.yggdrasil.execution.EvaluationContext

import scalaz._

trait PredicatePullupSpecs[M[+_]] extends EvaluatorSpecification[M] {
  import dag._

  val ctx = defaultEvaluationContext

  object pullups extends PredicatePullups with StdLibOpFinder {
    def MorphContext(ctx: EvaluationContext, node: DepGraph): MorphContext = new MorphContext(ctx, null)
  }
  import pullups._

  "Predicate pullups optimization" should {
    "pull a predicate out of a solve with a single ticvar" in {
      val rawInput = """
        | clicks := //clicks
        |
        | upperBound := 1329643873628
        | lowerBound := 1328866273610
        | extraLB := lowerBound - (24*60*60000)
        |
        | solve 'userId
        |   clicks' := clicks where clicks.time <= upperBound & clicks.time >= extraLB & clicks.userId = 'userId
        |   {userId: 'userId, time: clicks'.time}
        | """.stripMargin

      val loc = instructions.Line(1, 1, "")

      val load = AbsoluteLoad(Const(CString("/clicks"))(loc))(loc)

      val id = new Identifier

      val split =
        Split(
          Group(0,
            load,
            IntersectBucketSpec(
              IntersectBucketSpec(
                Extra(
                  Join(instructions.LtEq,Cross(None),
                    Join(instructions.DerefObject,Cross(None), load, Const(CString("time"))(loc))(loc),
                    Const(CLong(1329643873628L))(loc)
                  )(loc)
                ),
                Extra(
                  Join(instructions.GtEq,Cross(None),
                    Join(instructions.DerefObject,Cross(None), load, Const(CString("time"))(loc))(loc),
                    Const(CLong(1328779873610L))(loc)
                  )(loc)
                )
              ),
              UnfixedSolution(1,
                Join(instructions.DerefObject,Cross(None), load, Const(CString("userId"))(loc))(loc)
              )
            )
          ),
          Join(instructions.JoinObject,Cross(None),
            Join(instructions.WrapObject,Cross(None),
              Const(CString("userId"))(loc),
              SplitParam(1, id)(loc)
            )(loc),
            Join(instructions.WrapObject,Cross(None),
              Const(CString("time"))(loc),
              Join(instructions.DerefObject,Cross(None),
                SplitGroup(0, load.identities, id)(loc),
                Const(CString("time"))(loc)
              )(loc)
            )(loc)
          )(loc), id
        )(loc)

      val filteredLoad =
        Filter(IdentitySort,
          load,
          Join(instructions.And,IdentitySort,
            Join(instructions.LtEq,Cross(None),
              Join(instructions.DerefObject,Cross(None), load, Const(CString("time"))(loc))(loc),
              Const(CLong(1329643873628L))(loc)
            )(loc),
            Join(instructions.GtEq,Cross(None),
              Join(instructions.DerefObject,Cross(None), load, Const(CString("time"))(loc))(loc),
              Const(CLong(1328779873610L))(loc)
            )(loc)
          )(loc)
        )(loc)

      val expected =
        Split(
          Group(0,
            filteredLoad,
            UnfixedSolution(1,
              Join(instructions.DerefObject,Cross(None),
                filteredLoad,
                Const(CString("userId"))(loc)
              )(loc)
            )
          ),
          Join(instructions.JoinObject,Cross(None),
            Join(instructions.WrapObject,Cross(None),
              Const(CString("userId"))(loc),
              SplitParam(1, id)(loc)
            )(loc),
            Join(instructions.WrapObject,Cross(None),
              Const(CString("time"))(loc),
              Join(instructions.DerefObject,Cross(None),
                SplitGroup(0, load.identities, id)(loc),
                Const(CString("time"))(loc)
              )(loc)
            )(loc)
          )(loc), id
        )(loc)

      predicatePullups(split, ctx) mustEqual expected
    }

    "pull a predicate out of a solve with more than one ticvar" in {
      val rawInput = """
        | clicks := //clicks
        |
        | upperBound := 1329643873628
        | lowerBound := 1328866273610
        | extraLB := lowerBound - (24*60*60000)
        |
        | solve 'userId, 'pageId
        |   clicks' := clicks where clicks.time <= upperBound & clicks.time >= extraLB & clicks.userId = 'userId & clicks.pageId = 'pageId
        |   {userId: 'userId, time: clicks'.time}
        | """.stripMargin

      val id = new Identifier

      val loc = instructions.Line(1, 1, "")

      val load = AbsoluteLoad(Const(CString("/clicks"))(loc))(loc)

      val split =
        Split(
          Group(0,
            load,
            IntersectBucketSpec(
              IntersectBucketSpec(
                IntersectBucketSpec(
                  Extra(
                    Join(instructions.LtEq,Cross(None),
                      Join(instructions.DerefObject,Cross(None), load, Const(CString("time"))(loc))(loc),
                      Const(CLong(1329643873628L))(loc)
                    )(loc)
                  ),
                  Extra(
                    Join(instructions.GtEq,Cross(None),
                      Join(instructions.DerefObject,Cross(None), load, Const(CString("time"))(loc))(loc),
                      Const(CLong(1328779873610L))(loc)
                    )(loc)
                  )
                ),
                UnfixedSolution(1,
                  Join(instructions.DerefObject,Cross(None), load, Const(CString("userId"))(loc))(loc)
                )
              ),
              UnfixedSolution(2,
                Join(instructions.DerefObject,Cross(None), load, Const(CString("pageId"))(loc))(loc)
              )
            )
          ),
          Join(instructions.JoinObject,Cross(None),
            Join(instructions.WrapObject,Cross(None),
              Const(CString("userId"))(loc),
              SplitParam(1, id)(loc)
            )(loc),
            Join(instructions.WrapObject,Cross(None),
              Const(CString("time"))(loc),
              Join(instructions.DerefObject,Cross(None),
                SplitGroup(0, load.identities, id)(loc),
                Const(CString("time"))(loc)
              )(loc)
            )(loc)
          )(loc), id
        )(loc)

        val filteredLoad =
          Filter(IdentitySort,
            load,
            Join(instructions.And,IdentitySort,
              Join(instructions.LtEq,Cross(None),
                Join(instructions.DerefObject,Cross(None), load, Const(CString("time"))(loc))(loc),
                Const(CLong(1329643873628L))(loc)
              )(loc),
              Join(instructions.GtEq,Cross(None),
                Join(instructions.DerefObject,Cross(None), load, Const(CString("time"))(loc))(loc),
                Const(CLong(1328779873610L))(loc)
              )(loc)
            )(loc)
          )(loc)

        val expected =
          Split(
            Group(0,
              filteredLoad,
              IntersectBucketSpec(
                UnfixedSolution(1,
                  Join(instructions.DerefObject,Cross(None),
                    filteredLoad,
                    Const(CString("userId"))(loc)
                  )(loc)
                ),
                UnfixedSolution(2,
                  Join(instructions.DerefObject,Cross(None),
                    filteredLoad,
                    Const(CString("pageId"))(loc)
                  )(loc)
                )
              )
            ),
            Join(instructions.JoinObject,Cross(None),
              Join(instructions.WrapObject,Cross(None),
                Const(CString("userId"))(loc),
                SplitParam(1, id)(loc)
              )(loc),
              Join(instructions.WrapObject,Cross(None),
                Const(CString("time"))(loc),
                Join(instructions.DerefObject,Cross(None),
                  SplitGroup(0, load.identities, id)(loc),
                  Const(CString("time"))(loc)
                )(loc)
              )(loc)
            )(loc), id
          )(loc)

      predicatePullups(split, ctx) mustEqual expected
    }

    "pull a predicate out of the top level of a nested solve" in {
      val rawInput = """
        | medals := //summer_games/london_medals
        |
        | solve 'gender
        |   medals' := medals where medals.Gender = 'gender & medals.Edition = 2000
        |
        |   solve 'weight
        |     medals' where medals'.Weight = 'weight
        | """.stripMargin

      val id1 = new Identifier
      val id2 = new Identifier

      val loc = instructions.Line(1, 1, "")

      val load = AbsoluteLoad(Const(CString("/summer_games/london_medals"))(loc))(loc)

      lazy val split =
        Split(
          Group(0,
            load,
            IntersectBucketSpec(
              UnfixedSolution(1,
                Join(instructions.DerefObject,Cross(None), load, Const(CString("Gender"))(loc))(loc)
              ),
              Extra(
                Join(instructions.Eq,Cross(None),
                  Join(instructions.DerefObject,Cross(None), load, Const(CString("Edition"))(loc))(loc),
                  Const(CLong(2000))(loc)
                )(loc)
              )
            )
          ),
          innerSplit,
          id1
        )(loc)

      lazy val innerSplit =
        Split(
          Group(2,
            SplitGroup(0, load.identities, id1)(loc),
            UnfixedSolution(3,
              Join(instructions.DerefObject,Cross(None),
                SplitGroup(0, load.identities, id1)(loc),
                Const(CString("Weight"))(loc)
              )(loc)
            )
          ),
          SplitGroup(2, load.identities, id2)(loc),
          id2
        )(loc)

      val filteredLoad =
        Filter(IdentitySort,
          load,
          Join(instructions.Eq,Cross(None),
            Join(instructions.DerefObject,Cross(None), load, Const(CString("Edition"))(loc))(loc),
            Const(CLong(2000))(loc)
          )(loc)
        )(loc)

      lazy val expected =
        Split(
          Group(0,
            filteredLoad,
            UnfixedSolution(1,
              Join(instructions.DerefObject,Cross(None),
                filteredLoad,
                Const(CString("Gender"))(loc)
              )(loc)
            )
          ),
          expectedInner,
          id1
        )(loc)

      lazy val expectedInner =
        Split(
          Group(2,
            SplitGroup(0,
              load.identities,
              id1
            )(loc),
            UnfixedSolution(3,
              Join(instructions.DerefObject,Cross(None),
                SplitGroup(0, load.identities, id1)(loc),
                Const(CString("Weight"))(loc)
              )(loc)
            )
          ),
          SplitGroup(2, load.identities, id2)(loc),
          id2
        )(loc)

      predicatePullups(split, ctx) mustEqual expected
    }

    "pull a predicate out of a solve where both the filtered and the unfiltered set occur in the body" in {
      val rawInput = """
        | medals := //summer_games/london_medals
        |
        | solve 'gender
        |   medals' := medals where medals.Gender = 'gender & medals.Edition = 2000
        |   { gender1: 'gender, gender2: medals.Gender, gender3: medals'.Gender }
        | """.stripMargin

      val id = new Identifier

      val loc = instructions.Line(1, 1, "")

      val load = AbsoluteLoad(Const(CString("/summer_games/london_medals"))(loc))(loc)

      val split =
        Split(
          Group(0,
            load,
            IntersectBucketSpec(
              UnfixedSolution(1,
                Join(instructions.DerefObject,Cross(None), load, Const(CString("Gender"))(loc))(loc)
              ),
              Extra(
                Join(instructions.Eq,Cross(None),
                  Join(instructions.DerefObject,Cross(None), load, Const(CString("Edition"))(loc))(loc),
                  Const(CLong(2000))(loc)
                )(loc)
              )
            )
          ),
          Join(instructions.JoinObject,Cross(None),
            Join(instructions.WrapObject,Cross(None),
              Const(CString("gender1"))(loc),
              SplitParam(1, id)(loc)
            )(loc),
            Join(instructions.JoinObject,IdentitySort,
              Join(instructions.WrapObject,Cross(None),
                Const(CString("gender2"))(loc),
                Join(instructions.DerefObject,Cross(None),
                  load,
                  Const(CString("Gender"))(loc)
                )(loc)
              )(loc),
              Join(instructions.WrapObject,Cross(None),
                Const(CString("gender3"))(loc),
                Join(instructions.DerefObject,Cross(None),
                  SplitGroup(0,load.identities, id)(loc),
                  Const(CString("Gender"))(loc)
                )(loc)
              )(loc)
            )(loc)
          )(loc), id
        )(loc)

      val filteredLoad =
        Filter(IdentitySort,
          load,
          Join(instructions.Eq,Cross(None),
            Join(instructions.DerefObject,Cross(None), load, Const(CString("Edition"))(loc))(loc),
            Const(CLong(2000))(loc)
          )(loc)
        )(loc)

      val expected =
        Split(
          Group(0,
            filteredLoad,
            UnfixedSolution(1,
              Join(instructions.DerefObject,Cross(None),
                filteredLoad,
                Const(CString("Gender"))(loc)
              )(loc)
            )
          ),
          Join(instructions.JoinObject,Cross(None),
            Join(instructions.WrapObject,Cross(None),
              Const(CString("gender1"))(loc),
              SplitParam(1, id)(loc)
            )(loc),
            Join(instructions.JoinObject,IdentitySort,
              Join(instructions.WrapObject,Cross(None),
                Const(CString("gender2"))(loc),
                Join(instructions.DerefObject,Cross(None),
                  filteredLoad,
                  Const(CString("Gender"))(loc)
                )(loc)
              )(loc),
              Join(instructions.WrapObject,Cross(None),
                Const(CString("gender3"))(loc),
                Join(instructions.DerefObject,Cross(None),
                  SplitGroup(0, load.identities, id)(loc),
                  Const(CString("Gender"))(loc)
                )(loc)
              )(loc)
            )(loc)
          )(loc), id
        )(loc)

      predicatePullups(split, ctx) mustEqual expected
    }
  }
}

object PredicatePullupSpecs extends PredicatePullupSpecs[Need]
